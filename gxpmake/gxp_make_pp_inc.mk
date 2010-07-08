# 
# [1] define rule templates
# 

define expand_parameters
$(if $(1),.$$($(firstword $(1)))$(call expand_parameters,$(wordlist 2,$(words $(1)),$(1))))
endef

define expand_parameters_2
$(if $(1), $$(firstword $(1))=$($(firstword $(1)))$(call expand_parameters_2,$(wordlist 2,$(words $(1)),$(1))))
endef

define make_rule
$(output) : $(input)
	$(cmd)
endef

define make_rule_recursive
$(if $(1),\
  $(foreach $(firstword $(1)),$(or $($(firstword $(1))),""),$(call make_rule_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(eval $(call make_rule)))
endef

define make_dependence_recursive
$(if $(1),\
  $(foreach $(firstword $(1)),$(or $($(firstword $(1))),""),$(call make_dependence_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(output))
endef

define make_dependence
$(target) : $(call make_dependence_recursive,$(1))
endef

# 
# [2] set default parameters
# 

parameters:=a b c
target:=$(or $(target),gxp_pp_default_target)
ifeq ($(input),)
input=gxp_pp_default_input$(call expand_parameters,$(parameters))
endif
ifeq ($(output),)
output=gxp_pp_default_output$(call expand_parameters,$(parameters))
endif
ifeq ($(cmd),)
cmd=$(call expand_parameters_2,$(parameters)) echo
endif

#
# [3] really define rules
#
$(eval $(call make_dependence,$(parameters)))

$(eval $(call make_rule_recursive,$(parameters)))

# 
# [4] clear all variables
#
parameters:=
target:=
input=
output=
cmd=

