/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "includes.h"
#include "nc_iter.h"
#include "odom.h"

/**************************************************/
/* Code for generating data lists*/
/**************************************************/
/* For datalist constant rules: see the rules on the man page */

/* Forward*/
static void generate_array(Symbol*,Bytebuffer*,Datalist*,Generator*,Writer);
static void generate_arrayr(Symbol*,Bytebuffer*,Datalist*,Odometer*,int,Datalist*,Generator*);
static void generate_primdata(Symbol*, NCConstant*, Bytebuffer*, Datalist* fillsrc, Generator*);
static void generate_fieldarray(Symbol*, NCConstant*, Dimset*, Bytebuffer*, Datalist* fillsrc, Generator*);

/* Mnemonics */
#define VLENLIST1
#define FIELDARRAY 1

/**************************************************/


int
generator_getstate(Generator* generator ,void** statep)
{
    if(statep) *statep = (void*)generator->globalstate;
    return 1;
}

int generator_reset(Generator* generator, void* state)
{
    generator->globalstate = state;
    return 1;
}

#ifdef IGNORe
static void
checkodom(Odometer* odom)
{
    int i;
    for(i=0;i<odom->rank;i++) {
	ASSERT(odom->index[i] == odom->start[i]+odom->count[i]);
    }
}
#endif

/**************************************************/

void
generate_attrdata(Symbol* asym, Generator* generator, Writer writer, Bytebuffer* codebuf)
{
    Symbol* basetype = asym->typ.basetype;
    nc_type typecode = basetype->typ.typecode;

   if(typecode == NC_CHAR) {
	gen_charattr(asym->data,codebuf);
    } else {
	int uid;
	size_t count;
        generator->listbegin(generator,asym,NULL,LISTATTR,asym->data->length,codebuf,&uid);
        for(count=0;count<asym->data->length;count++) {
            NCConstant* con = datalistith(asym->data,count);
            generator->list(generator,asym,NULL,LISTATTR,uid,count,codebuf);
            generate_basetype(asym->typ.basetype,con,codebuf,NULL,generator);
	}
        generator->listend(generator,asym,NULL,LISTATTR,uid,count,codebuf);
    }
    writer(generator,asym,codebuf,0,NULL,NULL);
}

void
generate_vardata(Symbol* vsym, Generator* generator, Writer writer, Bytebuffer* code)
{
    Dimset* dimset = &vsym->typ.dimset;
    int rank = dimset->ndims;
    Symbol* basetype = vsym->typ.basetype;
    Datalist* filler = getfiller(vsym);

    if(vsym->data == NULL) return;

    /* give the buffer a running start to be large enough*/
    if(!bbSetalloc(code, nciterbuffersize))
      return;

    if(rank == 0) {/*scalar case*/
        NCConstant* c0 = datalistith(vsym->data,0);
        generate_basetype(basetype,c0,code,filler,generator);
        writer(generator,vsym,code,0,NULL,NULL);
    } else {/*rank > 0*/
#if 0
        /* First, create an odometer using all of the dimensions */
        odom = newodometer(dimset,NULL,NULL);
	start = odometerstartvector(odom);
	count = odometercountvector(odom);
#endif
	generate_array(vsym,code,filler,generator,writer);
    }
}


/**

The basic idea is to split the set of dimensions into
groups and iterate over each group by recursion.

A group is defined as the range of indices starting at an
unlimited dimension up to (but not including) the next
unlimited.

The first group starts at index 0, even if dimension 0 is not
unlimited.  The last group is everything from the last
unlimited dimension thru the last dimension (index rank-1).

*/

static void
generate_array(Symbol* vsym,
               Bytebuffer* code,
               Datalist* filler,
               Generator* generator,
	       Writer writer
              )
{
    Dimset* dimset = &vsym->typ.dimset;
    int rank = dimset->ndims;
    Symbol* basetype = vsym->typ.basetype;
    nc_type typecode = basetype->typ.typecode;
    nciter_t iter;
    int firstunlim = findunlimited(dimset,1);
    int nunlim = countunlimited(dimset);
    int isnc3unlim = (nunlim <= 1 && (firstunlim == 0 || firstunlim == rank)); /* netcdf-3 case of at most 1 unlim in 0th dimension */

    ASSERT(rank > 0);

    if(isnc3unlim) {
        /* Handle NC_CHAR case separately */
        if(typecode == NC_CHAR) {
            Odometer* odom = newodometer(dimset,NULL,NULL);
            Bytebuffer* charbuf = bbNew();
            gen_chararray(dimset,0,vsym->data,charbuf,filler);
	    generator->charconstant(generator,vsym,code,charbuf);
	    /* Create an odometer to get the dimension info */
            writer(generator,vsym,code,odom->rank,odom->start,odom->count);
#if 0
            writer(generator,vsym,code,odom->rank,0,bbLength(charbuf));
#endif
	    bbFree(charbuf);
    	    odometerfree(odom);
	} else { /* typecode != NC_CHAR */
            Odometer* odom = newodometer(dimset,NULL,NULL);
            /* Case: dim 1..rank-1 are not unlimited, dim 0 might be */
            size_t offset = 0; /* where are we in the data list */
            size_t nelems = 0; /* # of data list items to generate */
            /* Create an iterator and odometer and just walk the datalist */
            nc_get_iter(vsym,nciterbuffersize,&iter);
            for(;;offset+=nelems) {
                int i,uid;
                nelems=nc_next_iter(&iter,odometerstartvector(odom),odometercountvector(odom));
                if(nelems == 0)
		    break;
                bbClear(code);
                generator->listbegin(generator,vsym,NULL,LISTDATA,vsym->data->length,code,&uid);
                for(i=0;i<nelems;i++) {
                    NCConstant* con = datalistith(vsym->data,i+offset);
                    generator->list(generator,vsym,NULL,LISTDATA,uid,i,code);
                    generate_basetype(basetype,con,code,filler,generator);
                }
                generator->listend(generator,vsym,NULL,LISTDATA,uid,i,code);
                writer(generator,vsym,code,rank,odom->start,odom->count);
            }
	    odometerfree(odom);
	}
    } else { /* Hard case: multiple unlimited dimensions or unlim in dim > 0*/
        Odometer* odom = newodometer(dimset,NULL,NULL);
        /* Setup iterator and odometer */
        nc_get_iter(vsym,NC_MAX_UINT,&iter); /* effectively infinite */
        for(;;) {/* iterate in nelem chunks */
            /* get nelems count and modify odometer */
            size_t nelems=nc_next_iter(&iter,odom->start,odom->count);
            if(nelems == 0) break;
            generate_arrayr(vsym,code,vsym->data,
                            odom,
                            /*dim index=*/0,
                            filler,generator
                           );
            writer(generator,vsym,code,odom->rank,odom->start,odom->count);
        }
        odometerfree(odom);
    }
}

/**
The basic idea is to split the set of dimensions into groups
and iterate over each group.  A group is defined as the
range of indices starting at an unlimited dimension up to
(but not including) the next unlimited.  The first group
starts at index 0, even if dimension 0 is not unlimited.
The last group is everything from the last unlimited
dimension thru the last dimension (index rank-1).
*/
static void
generate_arrayr(Symbol* vsym,
               Bytebuffer* code,
               Datalist* list,
               Odometer* odom,
               int dimindex,
               Datalist* filler,
               Generator* generator
              )
{
    int uid,i;
    Symbol* basetype = vsym->typ.basetype;
    Dimset* dimset = &vsym->typ.dimset;
    int rank = dimset->ndims;
    int lastunlimited = findlastunlimited(dimset);
    int nextunlimited = findunlimited(dimset,dimindex+1);
    int typecode = basetype->typ.typecode;
    int islastgroup = (lastunlimited == rank || dimindex >= lastunlimited || dimindex == rank-1);
    Odometer* subodom = NULL;

    ASSERT(rank > 0);
    ASSERT((dimindex >= 0 && dimindex < rank));

    if(islastgroup) {
        /* Handle NC_CHAR case separately */
        if(typecode == NC_CHAR) {
            Bytebuffer* charbuf = bbNew();
            gen_chararray(dimset,dimindex,list,charbuf,filler);
	    generator->charconstant(generator,vsym,code,charbuf);
	    bbFree(charbuf);
	} else {
            /* build a special odometer to walk the last few dimensions */
            subodom = newsubodometer(odom,dimset,dimindex,rank);
            generator->listbegin(generator,vsym,NULL,LISTDATA,list->length,code,&uid);
            for(i=0;odometermore(subodom);i++) {
                size_t offset = odometeroffset(subodom);
                NCConstant* con = datalistith(list,offset);
                generator->list(generator,vsym,NULL,LISTDATA,uid,i,code);
                generate_basetype(basetype,con,code,filler,generator);
                odometerincr(subodom);
            }
            generator->listend(generator,vsym,NULL,LISTDATA,uid,i,code);
            odometerfree(subodom); subodom = NULL;
	}
    } else {/* !islastgroup */
        /* Our datalist must be a list of compounds representing
           the next unlimited; so walk the subarray from this index
           up to next unlimited.
        */
        ASSERT((dimindex < nextunlimited));
        ASSERT((isunlimited(dimset,nextunlimited)));
        /* build a sub odometer */
        subodom = newsubodometer(odom,dimset,dimindex,nextunlimited);
        for(i=0;odometermore(subodom);i++) {
            size_t offset = odometeroffset(subodom);
            NCConstant* con = datalistith(list,offset);
            if(con == NULL || con->nctype == NC_FILL) {
                if(filler == NULL)
                    filler = getfiller(vsym);
                generate_arrayr(vsym,code,filler,odom,nextunlimited,NULL,generator);

            } else if(islistconst(con)) {
                Datalist* sublist = compoundfor(con);
                generate_arrayr(vsym,code,sublist,odom,nextunlimited,filler,generator);
            } else {
                semerror(constline(con),"Expected {...} representing unlimited list");
                return;
            }
            odometerincr(subodom);
        }
        odometerfree(subodom); subodom = NULL;
    }
    if(subodom != NULL)
        odometerfree(subodom);
    return;
}

/* Generate an instance of the basetype using the value of con*/
void
generate_basetype(Symbol* tsym, NCConstant* con, Bytebuffer* codebuf, Datalist* filler, Generator* generator)
{
    Datalist* data;
    int offsetbase = 0;

    switch (tsym->subclass) {

    case NC_ENUM:
    case NC_OPAQUE:
    case NC_PRIM:
        if(islistconst(con)) {
            semerror(constline(con),"Expected primitive found {..}");
        }
        generate_primdata(tsym,con,codebuf,filler,generator);
        break;

    case NC_COMPOUND: {
        int i,uid, nfields, dllen;
        if(con == NULL || isfillconst(con)) {
            Datalist* fill = (filler==NULL?getfiller(tsym):filler);
	    ASSERT(fill->length == 1);
            con = fill->data[0];
            if(!islistconst(con)) {
              if(con)
                semerror(con->lineno,"Compound data fill value is not enclosed in {..}");
              else
                semerror(0,"Compound data fill value not enclosed in {..}, con is NULL.");
            }
        }

        if(!con) { /* fail on null compound. */
          semerror(constline(con),"NULL compound data.");
          break;
        }

        if(!islistconst(con)) {/* fail on no compound*/
            semerror(constline(con),"Compound data must be enclosed in {..}");
        }

        data = con->value.compoundv;
        nfields = listlength(tsym->subnodes);
        dllen = datalistlen(data);
        if(dllen > nfields) {
          semerror(con->lineno,"Datalist longer than the number of compound fields");
            break;
        }
        generator->listbegin(generator,tsym,&offsetbase,LISTCOMPOUND,listlength(tsym->subnodes),codebuf,&uid);
        for(i=0;i<nfields;i++) {
            Symbol* field = (Symbol*)listget(tsym->subnodes,i);
            con = datalistith(data,i);
            generator->list(generator,field,&offsetbase,LISTCOMPOUND,uid,i,codebuf);
            generate_basetype(field,con,codebuf,NULL,generator);
        }
        generator->listend(generator,tsym,&offsetbase,LISTCOMPOUND,uid,i,codebuf);
        } break;

    case NC_VLEN: {
        Bytebuffer* vlenbuf;
        int uid;
        size_t count;

        if(con == NULL || isfillconst(con)) {
            Datalist* fill = (filler==NULL?getfiller(tsym):filler);
            ASSERT(fill->length == 1);
            con = fill->data[0];
            if(con->nctype != NC_COMPOUND) {
                semerror(con->lineno,"Vlen data fill value is not enclosed in {..}");
            }
        }

        if(!islistconst(con)) {
            semerror(constline(con),"Vlen data must be enclosed in {..}");
        }
        data = con->value.compoundv;
        /* generate the nc_vlen_t instance*/
        vlenbuf = bbNew();
        if(tsym->typ.basetype->typ.typecode == NC_CHAR) {
            gen_charseq(data,vlenbuf);
            generator->vlenstring(generator,tsym,vlenbuf,&uid,&count);
        } else {
            generator->listbegin(generator,tsym,NULL,LISTVLEN,data->length,codebuf,&uid);
            for(count=0;count<data->length;count++) {
              NCConstant* con;
                generator->list(generator,tsym,NULL,LISTVLEN,uid,count,vlenbuf);
                con = datalistith(data,count);
                generate_basetype(tsym->typ.basetype,con,vlenbuf,NULL,generator);
            }
            generator->listend(generator,tsym,NULL,LISTVLEN,uid,count,codebuf,(void*)vlenbuf);
        }
        generator->vlendecl(generator,tsym,codebuf,uid,count,vlenbuf); /* Will extract contents of vlenbuf */
        bbFree(vlenbuf);
        } break;

    case NC_FIELD:
        if(tsym->typ.dimset.ndims > 0) {
            /* Verify that we have a sublist (or fill situation) */
            if(con != NULL && !isfillconst(con) && !islistconst(con))
                semerror(constline(con),"Dimensioned fields must be enclose in {...}");
            generate_fieldarray(tsym->typ.basetype,con,&tsym->typ.dimset,codebuf,filler,generator);
        } else {
            generate_basetype(tsym->typ.basetype,con,codebuf,NULL,generator);
        }
        break;

    default: PANIC1("generate_basetype: unexpected subclass %d",tsym->subclass);
    }
}

/* Used only for structure field arrays*/
static void
generate_fieldarray(Symbol* basetype, NCConstant* con, Dimset* dimset,
                 Bytebuffer* codebuf, Datalist* filler, Generator* generator)
{
    int i;
    int chartype = (basetype->typ.typecode == NC_CHAR);
    Datalist* data;
    int rank = rankfor(dimset);

    ASSERT(dimset->ndims > 0);

    if(con != NULL && !isfillconst(con))
        data = con->value.compoundv;
    else
        data = NULL;

    if(chartype) {
        Bytebuffer* charbuf = bbNew();
        gen_chararray(dimset,0,data,charbuf,filler);
        generator->charconstant(generator,basetype,codebuf,charbuf);
        bbFree(charbuf);
    } else {
        int uid;
        size_t xproduct = crossproduct(dimset,0,rank); /* compute total number of elements */
        generator->listbegin(generator,basetype,NULL,LISTFIELDARRAY,xproduct,codebuf,&uid);
        for(i=0;i<xproduct;i++) {
            con = (data == NULL ? NULL : datalistith(data,i));
            generator->list(generator,basetype,NULL,LISTFIELDARRAY,uid,i,codebuf);
            generate_basetype(basetype,con,codebuf,NULL,generator);
        }
        generator->listend(generator,basetype,NULL,LISTFIELDARRAY,uid,i,codebuf);
    }
}


/* An opaque string value might not conform
   to the size of the opaque to which it is being
   assigned. Normalize it to match the required
   opaque length (in bytes).
   Note that the string is a sequence of nibbles (4 bits).
*/
static void
normalizeopaquelength(NCConstant* prim, unsigned long nbytes)
{
    int nnibs = 2*nbytes;
    ASSERT(prim->nctype==NC_OPAQUE);
    if(prim->value.opaquev.len == nnibs) {
        /* do nothing*/
    } else if(prim->value.opaquev.len > nnibs) { /* truncate*/
        prim->value.opaquev.stringv[nnibs] = '\0';
        prim->value.opaquev.len = nnibs;
    } else {/* prim->value.opaquev.len < nnibs => expand*/
        char* s;
        s = (char*)ecalloc(nnibs+1);
        memset(s,'0',nnibs);    /* Fill with '0' characters */
        memcpy(s,prim->value.opaquev.stringv,prim->value.opaquev.len);
        s[nnibs] = '\0';
        efree(prim->value.opaquev.stringv);
        prim->value.opaquev.stringv=s;
        prim->value.opaquev.len = nnibs;
    }
}

static void
generate_primdata(Symbol* basetype, NCConstant* prim, Bytebuffer* codebuf,
                  Datalist* filler, Generator* generator)
{
    NCConstant* target;
    int match;

    if(prim == NULL || isfillconst(prim)) {
        Datalist* fill = (filler==NULL?getfiller(basetype):filler);
        ASSERT(fill->length == 1);
        prim = datalistith(fill,0);
    }

    ASSERT((prim->nctype != NC_COMPOUND));

    /* Verify that the constant is consistent with the type */
    match = 1;
    switch (prim->nctype) {
    case NC_CHAR:
    case NC_BYTE:
    case NC_SHORT:
    case NC_INT:
    case NC_FLOAT:
    case NC_DOUBLE:
    case NC_UBYTE:
    case NC_USHORT:
    case NC_UINT:
    case NC_INT64:
    case NC_UINT64:
    case NC_STRING:
        match = (basetype->subclass == NC_PRIM ? 1 : 0);
        break;

#ifdef USE_NETCDF4
    case NC_NIL:
        match = (basetype->subclass == NC_PRIM && basetype->typ.typecode == NC_STRING ? 1 : 0);
        break;

    case NC_OPAQUE:
        /* OPAQUE is also consistent with numbers */
        match = (basetype->subclass == NC_OPAQUE
                 || basetype->subclass == NC_PRIM ? 1 : 0);
        break;
    case NC_ECONST:
        match = (basetype->subclass == NC_ENUM ? 1 : 0);
        if(match) {
            /* Make sure this econst belongs to this enum */
            Symbol* ec = prim->value.enumv;
            Symbol* en = ec->container;
            match = (en == basetype);
        }
        break;
#endif
    default:
        match = 0;
    }
    if(!match) {
        semerror(constline(prim),"Data value is not consistent with the expected type: %s",
                 basetype->name);
    }

    target = nullconst();
    target->nctype = basetype->typ.typecode;

    if(target->nctype != NC_ECONST) {
        convert1(prim,target);
    }

    switch (target->nctype) {
    case NC_ECONST:
        if(basetype->subclass != NC_ENUM) {
            semerror(constline(prim),"Conversion to enum not supported (yet)");
        } break;
     case NC_OPAQUE:
        normalizeopaquelength(target,basetype->typ.size);
        break;
    default:
        break;
    }
    generator->constant(generator,basetype,target,codebuf);
    reclaimconstant(target);
    target = NULL;
    return;
}

/* Avoid long argument lists */
struct Args {
    Symbol* vsym;
    Dimset* dimset;
    int typecode;
    int storage;
    int rank;
    Generator* generator;
    Writer writer;
    Bytebuffer* code;
    Datalist* filler;
    size_t dimsizes[NC_MAX_VAR_DIMS];
    size_t chunksizes[NC_MAX_VAR_DIMS];
};

static void
generate_arrayR(struct Args* args, int dimindex, size_t* index, Datalist* data)
{
    size_t counter,stop;
    size_t count[NC_MAX_VAR_DIMS];
    Datalist* actual;
    Symbol* dim = args->dimset->dimsyms[dimindex];

    stop = args->dimsizes[dimindex];

    /* Four cases: (dimindex==rank-1|dimindex<rank-1) X (unlimited|!unlimited) */
    if(dimindex == (args->rank - 1)) {/* base case */
	int uid;
	if(dimindex > 0 && dim->dim.isunlimited) {
	    /* Get the unlimited list */
	    NCConstant* con = datalistith(data,0);
	    actual = compoundfor(con);    
	} else
	    actual = data;
        /* For last index, dump all of its elements */
        args->generator->listbegin(args->generator,args->vsym,NULL,LISTDATA,datalistlen(actual),args->code,&uid);
        for(counter=0;counter<stop;counter++) {
            NCConstant* con = datalistith(actual,counter);
            generate_basetype(args->vsym->typ.basetype,con,args->code,args->filler,args->generator);
            args->generator->list(args->generator,args->vsym,NULL,LISTDATA,uid,counter,args->code);
        }
        args->generator->listend(args->generator,args->vsym,NULL,LISTDATA,uid,counter,args->code);
        memcpy(count,onesvector,sizeof(size_t)*dimindex);
        count[dimindex] = stop;
        args->writer(args->generator,args->vsym,args->code,args->rank,index,count);
        bbClear(args->code);
    } else {    
        actual = data;
        /* Iterate over this dimension */
        for(counter = 0;counter < stop; counter++) {
            Datalist* subdata = NULL;
            NCConstant* con = datalistith(actual,counter);
	    if(con == NULL)
		subdata = filldatalist;
	    else {
	        ASSERT(islistconst(con));
	        if(islistconst(con)) subdata = compoundfor(con);
	    }
            index[dimindex] = counter;
            generate_arrayR(args,dimindex+1,index,subdata); /* recurse */
        }
    }
}

static void
generate_array(Symbol* vsym, Bytebuffer* code, Datalist* filler, Generator* generator, Writer writer)
{
    int i;
    size_t index[NC_MAX_VAR_DIMS];
    struct Args args;
    size_t totalsize;
    int nunlimited = 0;

    assert(vsym->typ.dimset.ndims > 0);

    args.vsym = vsym;
    args.dimset = &vsym->typ.dimset;
    args.generator = generator;
    args.writer = writer;
    args.filler = filler;
    args.code = code;
    args.rank = args.dimset->ndims;
    args.storage = vsym->var.special._Storage;
    args.typecode = vsym->typ.basetype->typ.typecode;

    assert(args.rank > 0);
    
    totalsize = 1; /* total # elements in the array */
    for(i=0;i<args.rank;i++) {
        args.dimsizes[i] = args.dimset->dimsyms[i]->dim.declsize;
	totalsize *= args.dimsizes[i];
    }
    nunlimited = countunlimited(args.dimset);

    if(vsym->var.special._Storage == NC_CHUNKED)
        memcpy(args.chunksizes,vsym->var.special._ChunkSizes,sizeof(size_t)*args.rank);

    memset(index,0,sizeof(index));

    /* Special case for NC_CHAR */
    if(args.typecode == NC_CHAR) {
        size_t start[NC_MAX_VAR_DIMS];
        size_t count[NC_MAX_VAR_DIMS];
        Bytebuffer* charbuf = bbNew();
        gen_chararray(args.dimset,0,args.vsym->data,charbuf,args.filler);
        args.generator->charconstant(args.generator,args.vsym,args.code,charbuf);
        memset(start,0,sizeof(size_t)*args.rank);
        memcpy(count,args.dimsizes,sizeof(size_t)*args.rank);
        args.writer(args.generator,args.vsym,args.code,args.rank,start,count);
        bbFree(charbuf);
        bbClear(args.code);
	return;
    }

    /* If the total no. of elements is less than some max and no unlimited,
       then generate a single vara that covers the whole array */
    if(totalsize <= wholevarsize && nunlimited == 0) {
	Symbol* basetype = args.vsym->typ.basetype;
	size_t counter;
	int uid;	
	Datalist* flat = flatten(vsym->data,args.rank);
        args.generator->listbegin(args.generator,basetype,NULL,LISTDATA,totalsize,args.code,&uid);
        for(counter=0;counter<totalsize;counter++) {
            NCConstant* con = datalistith(flat,counter);
	    if(con == NULL)
	        con = &fillconstant;
            generate_basetype(basetype,con,args.code,args.filler,args.generator);
            args.generator->list(args.generator,args.vsym,NULL,LISTDATA,uid,counter,args.code);
        }
        args.generator->listend(args.generator,args.vsym,NULL,LISTDATA,uid,counter,args.code);
        args.writer(args.generator,args.vsym,args.code,args.rank,zerosvector,args.dimsizes);
	freedatalist(flat);
    } else
        generate_arrayR(&args, 0, index, vsym->data);    
}
