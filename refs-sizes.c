#define MODULE_TAG(name) ref_sizes_##name
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "array.h"
#include "refs-sizes.h"

enum output_styles {
	psql_set_style, psql_arg_style, define_style, num_output_styles
} Output_Style = psql_set_style;

const char *const output_style_names[num_output_styles] = {
	"psql-set", "psql-args", "define"
};

enum output_styles match_output_style(const char *const name) {
	const enum output_styles default_style = psql_set_style;
	enum output_styles style = 0;	// whatever style's first
	if ( name == NULL )
		return default_style;
	for ( const char *const *names_ptr = output_style_names
			; names_ptr < RA_END(output_style_names)
			; ++names_ptr
			) {
		if ( 0 == strcasecmp(*names_ptr, name) )
			return style;
		++style;
	}
	return default_style;
}

void put_1st(void) { }

void put_var_int(char *var, long long val) {
	switch (Output_Style) {
	case psql_set_style: printf("\\set %s %Ld\n", var, val); break;
	case psql_arg_style: printf(" -v %s=%Ld", var, val); break;
	case define_style: printf("#define %s %Ld\n", var, val); break;
	default: assert( Output_Style < 0 );
	}
}

void put_var_str(char *var, char *val) {
	switch (Output_Style) {
	case psql_set_style: printf("\\set %s %s\n", var, val); break;
	case psql_arg_style: printf(" -v %s=%s", var, val); break;
	case define_style: printf("#define %s %s\n", var, val); break;
	default: assert( Output_Style < 0 );
	}
}

void put_last(void) {
	switch (Output_Style) {
	case psql_arg_style: printf("%s", " "); break;
	default: break;
	}
}

int main(int argc, char *argv[]) {
	Output_Style = match_output_style(argv[1]);
	put_1st();
	// Fundamentals:
	assert( __WORDSIZE == 32 ||  __WORDSIZE == 64 );
	put_var_int("BitsPerWord", __WORDSIZE);
	put_var_int("BytesPerWord", __WORDSIZE / 8);
	// PostgreSQL specific:
	put_var_str("WordIntsPG",  __WORDSIZE == 32 ? "int4" : "int8");
	put_var_str("WordAlignPG",  __WORDSIZE == 32 ? "int4" : "double");
	// Needed for PostgreSQL sequences:
	put_var_int("RefTagMax", REF_MAX_TAG);
	put_var_int("RefIdMin", REF_MIN_ID);
	put_var_int("RefIdMax", REF_MAX_ID);
	//  put_var_int("RefTagWidth", REF_TAG_WIDTH);
	//  put_var_int("CRefsTypeLength", __WORDSIZE == 32 ? 4 : 8);
	//  put_var_str("CRefsTypeAlign",  __WORDSIZE == 32 ? "int4" : "double");
	//  put_var_int("RefTypeLength",  __WORDSIZE == 32 ? 4 : 8);
	//  put_var_str("RefTypeAlign",  __WORDSIZE == 32 ? "int4" : "double");
	//  put_var_str("refs_as_ints",  __WORDSIZE == 32 ? "int4" : "int8");
	put_last();
	return 0;
}
