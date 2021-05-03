SET(libncpoco_SOURCES ncpoco.c cp_unix.c cp_win32_c ncpoco.h)

add_library(ncpoco OBJECT ${libncpoco_SOURCES})
