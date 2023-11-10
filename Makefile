# Makefile

MODULE_big = art
EXTENSION = art
DATA = art.control $(wildcard art--*.sql)
PGFILEDESC = "art index"

OBJS = art.o \
	   art_cost.o \
	   art_insert.o \
	   art_pageops.o \
	   art_scan.o \
	   art_utils.o \
	   art_vacuum.o \
	   art_validate.o

ifdef PG_CONFIG_PATH
PG_CONFIG= $(PG_CONFIG_PATH)
else
PG_CONFIG = pg_config
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

REGRESS =

all: art.so
