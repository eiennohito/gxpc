# 
# define rule templates
# 

define expand_parameters
$(if $(1),.$$($(firstword $(1)))$(call expand_parameters,$(wordlist 2,$(words $(1)),$(1))))
endef

define expand_parameters_2
$(if $(1), $$(firstword $(1))=$($(firstword $(1)))$(call expand_parameters_2,$(wordlist 2,$(words $(1)),$(1))))
endef

define make_rule
$(target) : $(output)
$(output) :
	$(cmd)
endef

define make_rule_recursive
$(if $(1),\
  $(foreach $(firstword $(1)),$(or $($(firstword $(1))),""),$(call make_rule_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(eval $(call make_rule)))
endef

# 
# set default parameters
# 

parameters:=a b c
target:=$(or $(target),gxp_make_pp_default_target)
output:=$(or $(output),output$(call expand_parameters,$(parameters)))
cmd:=$(or $(cmd),$(call expand_parameters_2,$(parameters)) echo)

#
# really define rules
#
$(eval $(call make_rule_recursive,$(parameters)))

