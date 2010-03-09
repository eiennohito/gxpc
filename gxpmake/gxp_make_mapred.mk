#
# gxp_make_mapred.mk --- Makefile to perform mapreduce-like
# computation using Makefile
#
# changeable parameters
#

input:=input
output:=output

reader:=ex_line_reader
mapper:=ex_word_count_mapper
reducer:=ex_count_reducer
n_mappers:=3
n_reducers:=2

partitioner:=ex_partitioner
exchanger:=ex_exchanger
sorter:=sort
combiner:=ex_count_reducer
merger:=sort -m

int_dir:=int_dir
keep_intermediates:=n
small_step:=n
ifeq ($(dbg),y)
keep_intermediates:=y
small_step:=y
endif


all : $(output)

.DELETE_ON_ERROR :

map_idxs:=$(shell seq 0 $(shell expr $(n_mappers) - 1))
reduce_idxs:=$(shell seq 0 $(shell expr $(n_reducers) - 1))
# reduce.1 reduce.2 ... reduce.R
reduce_files:=$(addprefix $(int_dir)/reduce.,$(reduce_idxs))

#
# map tasks.
# the i-th map task makes map.i.[1-R] from i-th fragment of input
#
#  read i-th fragment of input | ./map_task | generate map.i.1 ... map.i.R
#

$(int_dir) : $(input) 
	mkdir -p $(int_dir) 

define map_rule
# intermediate files common in small_step and big_step execution
ifneq ($(keep_intermediates),y)
.INTERMEDIATE : $(int_dir)/part.$(1)
endif

ifeq ($(small_step),y)
ifneq ($(keep_intermediates),y)
.INTERMEDIATE : $(int_dir)/read.$(1)
.INTERMEDIATE : $(int_dir)/map.$(1)
endif
# in small_step exueciton, read, map, and partition are all separate tasks
# read a part of the input file and let the reader generate the sub-file (read.$(1))
$(int_dir)/read.$(1) : $(int_dir) $(input)
	$(reader) $(input) $(1),$(n_mappers) > $$@
# mapper takes the sub-file and generate a key-value file (map.$(1))
$(int_dir)/map.$(1) : $(int_dir)/read.$(1) 
	cat $$^ | $(mapper) > $$@
# partitioner partitions the mapper-generated file and generate key-value files
# for each reducer
$(int_dir)/part.$(1) : $(int_dir)/map.$(1) 
	cat $$^ | $(partitioner) $(n_reducers) > $$@
else
# big_step execution, in which read, map, and partition are piped
$(int_dir)/part.$(1) : $(int_dir) $(input)
	$(reader) $(input) $(1),$(n_mappers) | $(mapper) | $(partitioner) $(n_reducers) > $$@
endif
endef

$(foreach m,$(map_idxs),\
  $(eval $(call map_rule,$(m))))

#
# reduce tasks.
# the i-th reduce task makes reduce.i from map.[1-M].i
#
# sort map.1.i ... map.M.i | ./reduce_task > reduce.i
#
#

define reduce_rule
ifneq ($(keep_intermediates),y)
.INTERMEDIATE : $(int_dir)/reduce.$(1)
endif
ifeq ($(small_step),y)
ifneq ($(keep_intermediates),y)
.INTERMEDIATE : $(int_dir)/exchanged.$(1)
.INTERMEDIATE : $(int_dir)/sort_br.$(1)
endif
$(int_dir)/exchanged.$(1) : $(foreach m,$(map_idxs),$(int_dir)/part.$(m))
	$(exchanger) $$^ $(1),$(n_reducers) > $$@
$(int_dir)/sort_br.$(1) : $(int_dir)/exchanged.$(1)
	cat $$^ | $(sorter) > $$@
$(int_dir)/reduce.$(1) : $(int_dir)/sort_br.$(1)
	cat $$^ | $(reducer) > $$@
else
$(int_dir)/reduce.$(1) : $(foreach m,$(map_idxs),$(int_dir)/part.$(m))
	$(exchanger) $$^ $(1),$(n_reducers) | $(sorter) | $(reducer) > $$@
endif
endef

$(foreach r,$(reduce_idxs),\
  $(eval $(call reduce_rule,$(r))))

#
# merge all reduce results
#

ifeq ($(merger),)
$(output) : $(reduce_files)
else
$(output) : $(reduce_files)
	$(merger) $^ > $@
endif

clean :
	rm -rf $(int_dir)

