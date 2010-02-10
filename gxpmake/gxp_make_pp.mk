# parameters=a b c d e f
output=output$(call expand_parameters,$(parameters))
cmd=$(call expand_parameters_2,$(parameters)) echo

all :

define expand_parameters
$(if $(1),.$$($(firstword $(1)))$(call expand_parameters,$(wordlist 2,$(words $(1)),$(1))))
endef

define expand_parameters_2
$(if $(1), $$(firstword $(1))=$($(firstword $(1)))$(call expand_parameters_2,$(wordlist 2,$(words $(1)),$(1))))
endef

define make_rule
all : $(output)
$(output) :
	$(cmd)
endef

define make_rule_recursive
$(if $(1),\
  $(foreach $(firstword $(1)),$(or $($(firstword $(1))),""),$(call make_rule_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(eval $(call make_rule)))
endef

$(eval $(call make_rule_recursive,$(parameters)))

