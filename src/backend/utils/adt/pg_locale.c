/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities
 *
 * Portions Copyright (c) 2002-2025, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/pg_locale.c
 *
 *-----------------------------------------------------------------------
 */

/*----------
 * Here is how the locale stuff is handled: LC_COLLATE and LC_CTYPE
 * are fixed at CREATE DATABASE time, stored in pg_database, and cannot
 * be changed. Thus, the effects of strcoll(), strxfrm(), isupper(),
 * toupper(), etc. are always in the same fixed locale.
 *
 * LC_MESSAGES is settable at run time and will take effect
 * immediately.
 *
 * The other categories, LC_MONETARY, LC_NUMERIC, and LC_TIME are
 * permanently set to "C", and then we use temporary locale_t
 * objects when we need to look up locale data based on the GUCs
 * of the same name.  Information is cached when the GUCs change.
 * The cached information is only used by the formatting functions
 * (to_char, etc.) and the money type.  For the user, this should all be
 * transparent.
 *----------
 */


#include "postgres.h"

#include <time.h>

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
<<<<<<< HEAD
#include "catalog/pg_control.h"
=======
#include "catalog/pg_database.h"
#include "common/hashfn.h"
>>>>>>> REL_18_BETA1_branch
#include "common/string.h"
#include "mb/pg_wchar.h"
#include "utils/faultinjector.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/guc_hooks.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"
#include "common/mdb_locale.h"

#ifdef WIN32
#include <shlwapi.h>
#endif

/* Error triggered for locale-sensitive subroutines */
#define		PGLOCALE_SUPPORT_ERROR(provider) \
	elog(ERROR, "unsupported collprovider for %s: %c", __func__, provider)

/*
 * This should be large enough that most strings will fit, but small enough
 * that we feel comfortable putting it on the stack
 */
#define		TEXTBUFLEN			1024

#define		MAX_L10N_DATA		80

/* pg_locale_builtin.c */
extern pg_locale_t create_pg_locale_builtin(Oid collid, MemoryContext context);
extern char *get_collation_actual_version_builtin(const char *collcollate);

/* pg_locale_icu.c */
#ifdef USE_ICU
extern UCollator *pg_ucol_open(const char *loc_str);
extern char *get_collation_actual_version_icu(const char *collcollate);
#endif
extern pg_locale_t create_pg_locale_icu(Oid collid, MemoryContext context);

/* pg_locale_libc.c */
extern pg_locale_t create_pg_locale_libc(Oid collid, MemoryContext context);
extern char *get_collation_actual_version_libc(const char *collcollate);

extern size_t strlower_builtin(char *dst, size_t dstsize, const char *src,
							   ssize_t srclen, pg_locale_t locale);
extern size_t strtitle_builtin(char *dst, size_t dstsize, const char *src,
							   ssize_t srclen, pg_locale_t locale);
extern size_t strupper_builtin(char *dst, size_t dstsize, const char *src,
							   ssize_t srclen, pg_locale_t locale);
extern size_t strfold_builtin(char *dst, size_t dstsize, const char *src,
							  ssize_t srclen, pg_locale_t locale);

extern size_t strlower_icu(char *dst, size_t dstsize, const char *src,
						   ssize_t srclen, pg_locale_t locale);
extern size_t strtitle_icu(char *dst, size_t dstsize, const char *src,
						   ssize_t srclen, pg_locale_t locale);
extern size_t strupper_icu(char *dst, size_t dstsize, const char *src,
						   ssize_t srclen, pg_locale_t locale);
extern size_t strfold_icu(char *dst, size_t dstsize, const char *src,
						  ssize_t srclen, pg_locale_t locale);

extern size_t strlower_libc(char *dst, size_t dstsize, const char *src,
							ssize_t srclen, pg_locale_t locale);
extern size_t strtitle_libc(char *dst, size_t dstsize, const char *src,
							ssize_t srclen, pg_locale_t locale);
extern size_t strupper_libc(char *dst, size_t dstsize, const char *src,
							ssize_t srclen, pg_locale_t locale);

/* GUC settings */
char	   *locale_ctype;
char	   *locale_messages;
char	   *locale_monetary;
char	   *locale_numeric;
char	   *locale_time;

int			icu_validation_level = WARNING;

/*
 * lc_time localization cache.
 *
 * We use only the first 7 or 12 entries of these arrays.  The last array
 * element is left as NULL for the convenience of outside code that wants
 * to sequentially scan these arrays.
 */
char	   *localized_abbrev_days[7 + 1];
char	   *localized_full_days[7 + 1];
char	   *localized_abbrev_months[12 + 1];
char	   *localized_full_months[12 + 1];

/* is the databases's LC_CTYPE the C locale? */
bool		database_ctype_is_c = false;

static pg_locale_t default_locale = NULL;

/* indicates whether locale information cache is valid */
static bool CurrentLocaleConvValid = false;
static bool CurrentLCTimeValid = false;

/* Cache for collation-related knowledge */

typedef struct
{
	Oid			collid;			/* hash key: pg_collation OID */
	pg_locale_t locale;			/* locale_t struct, or 0 if not valid */

	/* needed for simplehash */
	uint32		hash;
	char		status;
} collation_cache_entry;

#define SH_PREFIX		collation_cache
#define SH_ELEMENT_TYPE	collation_cache_entry
#define SH_KEY_TYPE		Oid
#define SH_KEY			collid
#define SH_HASH_KEY(tb, key)   	murmurhash32((uint32) key)
#define SH_EQUAL(tb, a, b)		(a == b)
#define SH_GET_HASH(tb, a)		a->hash
#define SH_SCOPE		static inline
#define SH_STORE_HASH
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

static MemoryContext CollationCacheContext = NULL;
static collation_cache_hash *CollationCache = NULL;

/*
 * The collation cache is often accessed repeatedly for the same collation, so
 * remember the last one used.
 */
static Oid	last_collation_cache_oid = InvalidOid;
static pg_locale_t last_collation_cache_locale = NULL;

#if defined(WIN32) && defined(LC_MESSAGES)
static char *IsoLocaleName(const char *);
#endif

<<<<<<< HEAD
#ifdef USE_ICU
/*
 * Converter object for converting between ICU's UChar strings and C strings
 * in database encoding.  Since the database encoding doesn't change, we only
 * need one of these per session.
 */
static UConverter *icu_converter = NULL;

static UCollator *pg_ucol_open(const char *loc_str);
static void init_icu_converter(void);
static int32_t uchar_length(UConverter *converter,
							const char *str, int32_t len);
static int32_t uchar_convert(UConverter *converter,
							 UChar *dest, int32_t destlen,
							 const char *src, int32_t srclen);
static void icu_set_collation_attributes(UCollator *collator, const char *loc,
										 UErrorCode *status);
#endif

=======
>>>>>>> REL_18_BETA1_branch
/*
 * pg_perm_setlocale
 *
 * This wraps the libc function setlocale(), with two additions.  First, when
 * changing LC_CTYPE, update gettext's encoding for the current message
 * domain.  GNU gettext automatically tracks LC_CTYPE on most platforms, but
 * not on Windows.  Second, if the operation is successful, the corresponding
 * LC_XXX environment variable is set to match.  By setting the environment
 * variable, we ensure that any subsequent use of setlocale(..., "") will
 * preserve the settings made through this routine.  Of course, LC_ALL must
 * also be unset to fully ensure that, but that has to be done elsewhere after
 * all the individual LC_XXX variables have been set correctly.  (Thank you
 * Perl for making this kluge necessary.)
 */
char *
pg_perm_setlocale(int category, const char *locale)
{
	char	   *result;
	const char *envvar;

#ifndef WIN32
	result = SETLOCALE(category, locale);
#else

	/*
	 * On Windows, setlocale(LC_MESSAGES) does not work, so just assume that
	 * the given value is good and set it in the environment variables. We
	 * must ignore attempts to set to "", which means "keep using the old
	 * environment value".
	 */
#ifdef LC_MESSAGES
	if (category == LC_MESSAGES)
	{
		result = (char *) locale;
		if (locale == NULL || locale[0] == '\0')
			return result;
	}
	else
#endif
		result = SETLOCALE(category, locale);
#endif							/* WIN32 */

	if (result == NULL)
		return result;			/* fall out immediately on failure */

	/*
	 * Use the right encoding in translated messages.  Under ENABLE_NLS, let
	 * pg_bind_textdomain_codeset() figure it out.  Under !ENABLE_NLS, message
	 * format strings are ASCII, but database-encoding strings may enter the
	 * message via %s.  This makes the overall message encoding equal to the
	 * database encoding.
	 */
	if (category == LC_CTYPE)
	{
		static char save_lc_ctype[LOCALE_NAME_BUFLEN];

		/* copy setlocale() return value before callee invokes it again */
		strlcpy(save_lc_ctype, result, sizeof(save_lc_ctype));
		result = save_lc_ctype;

#ifdef ENABLE_NLS
		SetMessageEncoding(pg_bind_textdomain_codeset(textdomain(NULL)));
#else
		SetMessageEncoding(GetDatabaseEncoding());
#endif
	}

	switch (category)
	{
		case LC_COLLATE:
			envvar = "LC_COLLATE";
			break;
		case LC_CTYPE:
			envvar = "LC_CTYPE";
			break;
#ifdef LC_MESSAGES
		case LC_MESSAGES:
			envvar = "LC_MESSAGES";
#ifdef WIN32
			result = IsoLocaleName(locale);
			if (result == NULL)
				result = (char *) locale;
			elog(DEBUG3, "IsoLocaleName() executed; locale: \"%s\"", result);
#endif							/* WIN32 */
			break;
#endif							/* LC_MESSAGES */
		case LC_MONETARY:
			envvar = "LC_MONETARY";
			break;
		case LC_NUMERIC:
			envvar = "LC_NUMERIC";
			break;
		case LC_TIME:
			envvar = "LC_TIME";
			break;
		default:
			elog(FATAL, "unrecognized LC category: %d", category);
			return NULL;		/* keep compiler quiet */
	}

	if (setenv(envvar, result, 1) != 0)
		return NULL;

	return result;
}


/*
 * Is the locale name valid for the locale category?
 *
 * If successful, and canonname isn't NULL, a palloc'd copy of the locale's
 * canonical name is stored there.  This is especially useful for figuring out
 * what locale name "" means (ie, the server environment value).  (Actually,
 * it seems that on most implementations that's the only thing it's good for;
 * we could wish that setlocale gave back a canonically spelled version of
 * the locale name, but typically it doesn't.)
 */
bool
check_locale(int category, const char *locale, char **canonname)
{
	char	   *save;
	char	   *res;

	/* Don't let Windows' non-ASCII locale names in. */
	if (!pg_is_ascii(locale))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("locale name \"%s\" contains non-ASCII characters",
						locale)));
		return false;
	}

	if (canonname)
		*canonname = NULL;		/* in case of failure */

	save = SETLOCALE(category, NULL);
	if (!save)
		return false;			/* won't happen, we hope */

	/* save may be pointing at a modifiable scratch variable, see above. */
	save = pstrdup(save);

	/* set the locale with setlocale, to see if it accepts it. */
	res = SETLOCALE(category, locale);

	/* save canonical name if requested. */
	if (res && canonname)
		*canonname = pstrdup(res);

	/* restore old value. */
	if (!SETLOCALE(category, save))
		elog(WARNING, "failed to restore old locale \"%s\"", save);
	pfree(save);

	/* Don't let Windows' non-ASCII locale names out. */
	if (canonname && *canonname && !pg_is_ascii(*canonname))
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("locale name \"%s\" contains non-ASCII characters",
						*canonname)));
		pfree(*canonname);
		*canonname = NULL;
		return false;
	}

	return (res != NULL);
}


/*
 * GUC check/assign hooks
 *
 * For most locale categories, the assign hook doesn't actually set the locale
 * permanently, just reset flags so that the next use will cache the
 * appropriate values.  (See explanation at the top of this file.)
 *
 * Note: we accept value = "" as selecting the postmaster's environment
 * value, whatever it was (so long as the environment setting is legal).
 * This will have been locked down by an earlier call to pg_perm_setlocale.
 */
bool
check_locale_monetary(char **newval, void **extra, GucSource source)
{
	return check_locale(LC_MONETARY, *newval, NULL);
}

void
assign_locale_monetary(const char *newval, void *extra)
{
	CurrentLocaleConvValid = false;
}

bool
check_locale_numeric(char **newval, void **extra, GucSource source)
{
	return check_locale(LC_NUMERIC, *newval, NULL);
}

void
assign_locale_numeric(const char *newval, void *extra)
{
	CurrentLocaleConvValid = false;
}

bool
check_locale_time(char **newval, void **extra, GucSource source)
{
	return check_locale(LC_TIME, *newval, NULL);
}

void
assign_locale_time(const char *newval, void *extra)
{
	CurrentLCTimeValid = false;
}

/*
 * We allow LC_MESSAGES to actually be set globally.
 *
 * Note: we normally disallow value = "" because it wouldn't have consistent
 * semantics (it'd effectively just use the previous value).  However, this
 * is the value passed for PGC_S_DEFAULT, so don't complain in that case,
 * not even if the attempted setting fails due to invalid environment value.
 * The idea there is just to accept the environment setting *if possible*
 * during startup, until we can read the proper value from postgresql.conf.
 */
bool
check_locale_messages(char **newval, void **extra, GucSource source)
{
	if (**newval == '\0')
	{
		if (source == PGC_S_DEFAULT)
			return true;
		else
			return false;
	}

	/*
	 * LC_MESSAGES category does not exist everywhere, but accept it anyway
	 *
	 * On Windows, we can't even check the value, so accept blindly
	 */
#if defined(LC_MESSAGES) && !defined(WIN32)
	return check_locale(LC_MESSAGES, *newval, NULL);
#else
	return true;
#endif
}

void
assign_locale_messages(const char *newval, void *extra)
{
	/*
	 * LC_MESSAGES category does not exist everywhere, but accept it anyway.
	 * We ignore failure, as per comment above.
	 */
#ifdef LC_MESSAGES
	(void) pg_perm_setlocale(LC_MESSAGES, newval);
#endif
}


/*
 * Frees the malloced content of a struct lconv.  (But not the struct
 * itself.)  It's important that this not throw elog(ERROR).
 */
static void
free_struct_lconv(struct lconv *s)
{
	free(s->decimal_point);
	free(s->thousands_sep);
	free(s->grouping);
	free(s->int_curr_symbol);
	free(s->currency_symbol);
	free(s->mon_decimal_point);
	free(s->mon_thousands_sep);
	free(s->mon_grouping);
	free(s->positive_sign);
	free(s->negative_sign);
}

/*
 * Check that all fields of a struct lconv (or at least, the ones we care
 * about) are non-NULL.  The field list must match free_struct_lconv().
 */
static bool
struct_lconv_is_valid(struct lconv *s)
{
	if (s->decimal_point == NULL)
		return false;
	if (s->thousands_sep == NULL)
		return false;
	if (s->grouping == NULL)
		return false;
	if (s->int_curr_symbol == NULL)
		return false;
	if (s->currency_symbol == NULL)
		return false;
	if (s->mon_decimal_point == NULL)
		return false;
	if (s->mon_thousands_sep == NULL)
		return false;
	if (s->mon_grouping == NULL)
		return false;
	if (s->positive_sign == NULL)
		return false;
	if (s->negative_sign == NULL)
		return false;
	return true;
}


/*
 * Convert the strdup'd string at *str from the specified encoding to the
 * database encoding.
 */
static void
db_encoding_convert(int encoding, char **str)
{
	char	   *pstr;
	char	   *mstr;

	/* convert the string to the database encoding */
	pstr = pg_any_to_server(*str, strlen(*str), encoding);
	if (pstr == *str)
		return;					/* no conversion happened */

	/* need it malloc'd not palloc'd */
	mstr = strdup(pstr);
	if (mstr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	/* replace old string */
	free(*str);
	*str = mstr;

	pfree(pstr);
}


/*
 * Return the POSIX lconv struct (contains number/money formatting
 * information) with locale information for all categories.
 */
struct lconv *
PGLC_localeconv(void)
{
	static struct lconv CurrentLocaleConv;
	static bool CurrentLocaleConvAllocated = false;
	struct lconv *extlconv;
	struct lconv tmp;
	struct lconv worklconv = {0};

	/* Did we do it already? */
	if (CurrentLocaleConvValid)
		return &CurrentLocaleConv;

	/* Free any already-allocated storage */
	if (CurrentLocaleConvAllocated)
	{
		free_struct_lconv(&CurrentLocaleConv);
		CurrentLocaleConvAllocated = false;
	}

	/*
	 * Use thread-safe method of obtaining a copy of lconv from the operating
	 * system.
	 */
	if (pg_localeconv_r(locale_monetary,
						locale_numeric,
						&tmp) != 0)
		elog(ERROR,
			 "could not get lconv for LC_MONETARY = \"%s\", LC_NUMERIC = \"%s\": %m",
			 locale_monetary, locale_numeric);

<<<<<<< HEAD
	/* Save prevailing values of monetary and numeric locales */
	save_lc_monetary = SETLOCALE(LC_MONETARY, NULL);
	if (!save_lc_monetary)
		elog(ERROR, "setlocale(NULL) failed");
	save_lc_monetary = pstrdup(save_lc_monetary);

	save_lc_numeric = SETLOCALE(LC_NUMERIC, NULL);
	if (!save_lc_numeric)
		elog(ERROR, "setlocale(NULL) failed");
	save_lc_numeric = pstrdup(save_lc_numeric);

#ifdef WIN32

	/*
	 * The POSIX standard explicitly says that it is undefined what happens if
	 * LC_MONETARY or LC_NUMERIC imply an encoding (codeset) different from
	 * that implied by LC_CTYPE.  In practice, all Unix-ish platforms seem to
	 * believe that localeconv() should return strings that are encoded in the
	 * codeset implied by the LC_MONETARY or LC_NUMERIC locale name.  Hence,
	 * once we have successfully collected the localeconv() results, we will
	 * convert them from that codeset to the desired server encoding.
	 *
	 * Windows, of course, resolutely does things its own way; on that
	 * platform LC_CTYPE has to match LC_MONETARY/LC_NUMERIC to get sane
	 * results.  Hence, we must temporarily set that category as well.
	 */

	/* Save prevailing value of ctype locale */
	save_lc_ctype = SETLOCALE(LC_CTYPE, NULL);
	if (!save_lc_ctype)
		elog(ERROR, "setlocale(NULL) failed");
	save_lc_ctype = pstrdup(save_lc_ctype);

	/* Here begins the critical section where we must not throw error */

	/* use numeric to set the ctype */
	SETLOCALE(LC_CTYPE, locale_numeric);
#endif

	/* Get formatting information for numeric */
	SETLOCALE(LC_NUMERIC, locale_numeric);
	extlconv = localeconv();

	/* Must copy data now in case setlocale() overwrites it */
	worklconv.decimal_point = strdup(extlconv->decimal_point);
	worklconv.thousands_sep = strdup(extlconv->thousands_sep);
	worklconv.grouping = strdup(extlconv->grouping);

#ifdef WIN32
	/* use monetary to set the ctype */
	SETLOCALE(LC_CTYPE, locale_monetary);
#endif

	/* Get formatting information for monetary */
	SETLOCALE(LC_MONETARY, locale_monetary);
	extlconv = localeconv();

	/* Must copy data now in case setlocale() overwrites it */
=======
	/* Must copy data now so we can re-encode it. */
	extlconv = &tmp;
	worklconv.decimal_point = strdup(extlconv->decimal_point);
	worklconv.thousands_sep = strdup(extlconv->thousands_sep);
	worklconv.grouping = strdup(extlconv->grouping);
>>>>>>> REL_18_BETA1_branch
	worklconv.int_curr_symbol = strdup(extlconv->int_curr_symbol);
	worklconv.currency_symbol = strdup(extlconv->currency_symbol);
	worklconv.mon_decimal_point = strdup(extlconv->mon_decimal_point);
	worklconv.mon_thousands_sep = strdup(extlconv->mon_thousands_sep);
	worklconv.mon_grouping = strdup(extlconv->mon_grouping);
	worklconv.positive_sign = strdup(extlconv->positive_sign);
	worklconv.negative_sign = strdup(extlconv->negative_sign);
	/* Copy scalar fields as well */
	worklconv.int_frac_digits = extlconv->int_frac_digits;
	worklconv.frac_digits = extlconv->frac_digits;
	worklconv.p_cs_precedes = extlconv->p_cs_precedes;
	worklconv.p_sep_by_space = extlconv->p_sep_by_space;
	worklconv.n_cs_precedes = extlconv->n_cs_precedes;
	worklconv.n_sep_by_space = extlconv->n_sep_by_space;
	worklconv.p_sign_posn = extlconv->p_sign_posn;
	worklconv.n_sign_posn = extlconv->n_sign_posn;

<<<<<<< HEAD
	/*
	 * Restore the prevailing locale settings; failure to do so is fatal.
	 * Possibly we could limp along with nondefault LC_MONETARY or LC_NUMERIC,
	 * but proceeding with the wrong value of LC_CTYPE would certainly be bad
	 * news; and considering that the prevailing LC_MONETARY and LC_NUMERIC
	 * are almost certainly "C", there's really no reason that restoring those
	 * should fail.
	 */
#ifdef WIN32
	if (!SETLOCALE(LC_CTYPE, save_lc_ctype))
		elog(FATAL, "failed to restore LC_CTYPE to \"%s\"", save_lc_ctype);
#endif
	if (!SETLOCALE(LC_MONETARY, save_lc_monetary))
		elog(FATAL, "failed to restore LC_MONETARY to \"%s\"", save_lc_monetary);
	if (!SETLOCALE(LC_NUMERIC, save_lc_numeric))
		elog(FATAL, "failed to restore LC_NUMERIC to \"%s\"", save_lc_numeric);
=======
	/* Free the contents of the object populated by pg_localeconv_r(). */
	pg_localeconv_free(&tmp);

	/* If any of the preceding strdup calls failed, complain now. */
	if (!struct_lconv_is_valid(&worklconv))
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
>>>>>>> REL_18_BETA1_branch

	PG_TRY();
	{
		int			encoding;

		/*
		 * Now we must perform encoding conversion from whatever's associated
		 * with the locales into the database encoding.  If we can't identify
		 * the encoding implied by LC_NUMERIC or LC_MONETARY (ie we get -1),
		 * use PG_SQL_ASCII, which will result in just validating that the
		 * strings are OK in the database encoding.
		 */
		encoding = pg_get_encoding_from_locale(locale_numeric, true);
		if (encoding < 0)
			encoding = PG_SQL_ASCII;

		db_encoding_convert(encoding, &worklconv.decimal_point);
		db_encoding_convert(encoding, &worklconv.thousands_sep);
		/* grouping is not text and does not require conversion */

		encoding = pg_get_encoding_from_locale(locale_monetary, true);
		if (encoding < 0)
			encoding = PG_SQL_ASCII;

		db_encoding_convert(encoding, &worklconv.int_curr_symbol);
		db_encoding_convert(encoding, &worklconv.currency_symbol);
		db_encoding_convert(encoding, &worklconv.mon_decimal_point);
		db_encoding_convert(encoding, &worklconv.mon_thousands_sep);
		/* mon_grouping is not text and does not require conversion */
		db_encoding_convert(encoding, &worklconv.positive_sign);
		db_encoding_convert(encoding, &worklconv.negative_sign);
	}
	PG_CATCH();
	{
		free_struct_lconv(&worklconv);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Everything is good, so save the results.
	 */
	CurrentLocaleConv = worklconv;
	CurrentLocaleConvAllocated = true;
	CurrentLocaleConvValid = true;
	return &CurrentLocaleConv;
}

#ifdef WIN32
/*
 * On Windows, strftime() returns its output in encoding CP_ACP (the default
 * operating system codepage for the computer), which is likely different
 * from SERVER_ENCODING.  This is especially important in Japanese versions
 * of Windows which will use SJIS encoding, which we don't support as a
 * server encoding.
 *
 * So, instead of using strftime(), use wcsftime() to return the value in
 * wide characters (internally UTF16) and then convert to UTF8, which we
 * know how to handle directly.
 *
 * Note that this only affects the calls to strftime() in this file, which are
 * used to get the locale-aware strings. Other parts of the backend use
 * pg_strftime(), which isn't locale-aware and does not need to be replaced.
 */
static size_t
strftime_l_win32(char *dst, size_t dstlen,
				 const char *format, const struct tm *tm, locale_t locale)
{
	size_t		len;
	wchar_t		wformat[8];		/* formats used below need 3 chars */
	wchar_t		wbuf[MAX_L10N_DATA];

	/*
	 * Get a wchar_t version of the format string.  We only actually use
	 * plain-ASCII formats in this file, so we can say that they're UTF8.
	 */
	len = MultiByteToWideChar(CP_UTF8, 0, format, -1,
							  wformat, lengthof(wformat));
	if (len == 0)
		elog(ERROR, "could not convert format string from UTF-8: error code %lu",
			 GetLastError());

	len = _wcsftime_l(wbuf, MAX_L10N_DATA, wformat, tm, locale);
	if (len == 0)
	{
		/*
		 * wcsftime failed, possibly because the result would not fit in
		 * MAX_L10N_DATA.  Return 0 with the contents of dst unspecified.
		 */
		return 0;
	}

	len = WideCharToMultiByte(CP_UTF8, 0, wbuf, len, dst, dstlen - 1,
							  NULL, NULL);
	if (len == 0)
		elog(ERROR, "could not convert string to UTF-8: error code %lu",
			 GetLastError());

	dst[len] = '\0';

	return len;
}

/* redefine strftime_l() */
#define strftime_l(a,b,c,d,e) strftime_l_win32(a,b,c,d,e)
#endif							/* WIN32 */

/*
 * Subroutine for cache_locale_time().
 * Convert the given string from encoding "encoding" to the database
 * encoding, and store the result at *dst, replacing any previous value.
 */
static void
cache_single_string(char **dst, const char *src, int encoding)
{
	char	   *ptr;
	char	   *olddst;

	/* Convert the string to the database encoding, or validate it's OK */
	ptr = pg_any_to_server(src, strlen(src), encoding);

	/* Store the string in long-lived storage, replacing any previous value */
	olddst = *dst;
	*dst = MemoryContextStrdup(TopMemoryContext, ptr);
	if (olddst)
		pfree(olddst);

	/* Might as well clean up any palloc'd conversion result, too */
	if (ptr != src)
		pfree(ptr);
}

/*
 * Update the lc_time localization cache variables if needed.
 */
void
cache_locale_time(void)
{
	char		buf[(2 * 7 + 2 * 12) * MAX_L10N_DATA];
	char	   *bufptr;
	time_t		timenow;
	struct tm  *timeinfo;
	struct tm	timeinfobuf;
	bool		strftimefail = false;
	int			encoding;
	int			i;
	locale_t	locale;

	/* did we do this already? */
	if (CurrentLCTimeValid)
		return;

	elog(DEBUG3, "cache_locale_time() executed; locale: \"%s\"", locale_time);

<<<<<<< HEAD
	/*
	 * As in PGLC_localeconv(), it's critical that we not throw error while
	 * libc's locale settings have nondefault values.  Hence, we just call
	 * strftime() within the critical section, and then convert and save its
	 * results afterwards.
	 */

	/* Save prevailing value of time locale */
	save_lc_time = SETLOCALE(LC_TIME, NULL);
	if (!save_lc_time)
		elog(ERROR, "setlocale(NULL) failed");
	save_lc_time = pstrdup(save_lc_time);

#ifdef WIN32

	/*
	 * On Windows, it appears that wcsftime() internally uses LC_CTYPE, so we
	 * must set it here.  This code looks the same as what PGLC_localeconv()
	 * does, but the underlying reason is different: this does NOT determine
	 * the encoding we'll get back from strftime_win32().
	 */

	/* Save prevailing value of ctype locale */
	save_lc_ctype = SETLOCALE(LC_CTYPE, NULL);
	if (!save_lc_ctype)
		elog(ERROR, "setlocale(NULL) failed");
	save_lc_ctype = pstrdup(save_lc_ctype);

	/* use lc_time to set the ctype */
	SETLOCALE(LC_CTYPE, locale_time);
#endif

	SETLOCALE(LC_TIME, locale_time);
=======
	errno = ENOENT;
#ifdef WIN32
	locale = _create_locale(LC_ALL, locale_time);
	if (locale == (locale_t) 0)
		_dosmaperr(GetLastError());
#else
	locale = newlocale(LC_ALL_MASK, locale_time, (locale_t) 0);
#endif
	if (!locale)
		report_newlocale_failure(locale_time);
>>>>>>> REL_18_BETA1_branch

	/* We use times close to current time as data for strftime(). */
	timenow = time(NULL);
	timeinfo = gmtime_r(&timenow, &timeinfobuf);

	/* Store the strftime results in MAX_L10N_DATA-sized portions of buf[] */
	bufptr = buf;

	/*
	 * MAX_L10N_DATA is sufficient buffer space for every known locale, and
	 * POSIX defines no strftime() errors.  (Buffer space exhaustion is not an
	 * error.)  An implementation might report errors (e.g. ENOMEM) by
	 * returning 0 (or, less plausibly, a negative value) and setting errno.
	 * Report errno just in case the implementation did that, but clear it in
	 * advance of the calls so we don't emit a stale, unrelated errno.
	 */
	errno = 0;

	/* localized days */
	for (i = 0; i < 7; i++)
	{
		timeinfo->tm_wday = i;
		if (strftime_l(bufptr, MAX_L10N_DATA, "%a", timeinfo, locale) <= 0)
			strftimefail = true;
		bufptr += MAX_L10N_DATA;
		if (strftime_l(bufptr, MAX_L10N_DATA, "%A", timeinfo, locale) <= 0)
			strftimefail = true;
		bufptr += MAX_L10N_DATA;
	}

	/* localized months */
	for (i = 0; i < 12; i++)
	{
		timeinfo->tm_mon = i;
		timeinfo->tm_mday = 1;	/* make sure we don't have invalid date */
		if (strftime_l(bufptr, MAX_L10N_DATA, "%b", timeinfo, locale) <= 0)
			strftimefail = true;
		bufptr += MAX_L10N_DATA;
		if (strftime_l(bufptr, MAX_L10N_DATA, "%B", timeinfo, locale) <= 0)
			strftimefail = true;
		bufptr += MAX_L10N_DATA;
	}

#ifdef WIN32
<<<<<<< HEAD
	if (!SETLOCALE(LC_CTYPE, save_lc_ctype))
		elog(FATAL, "failed to restore LC_CTYPE to \"%s\"", save_lc_ctype);
#endif
	if (!SETLOCALE(LC_TIME, save_lc_time))
		elog(FATAL, "failed to restore LC_TIME to \"%s\"", save_lc_time);
=======
	_free_locale(locale);
#else
	freelocale(locale);
#endif
>>>>>>> REL_18_BETA1_branch

	/*
	 * At this point we've done our best to clean up, and can throw errors, or
	 * call functions that might throw errors, with a clean conscience.
	 */
	if (strftimefail)
		elog(ERROR, "strftime_l() failed");

#ifndef WIN32

	/*
	 * As in PGLC_localeconv(), we must convert strftime()'s output from the
	 * encoding implied by LC_TIME to the database encoding.  If we can't
	 * identify the LC_TIME encoding, just perform encoding validation.
	 */
	encoding = pg_get_encoding_from_locale(locale_time, true);
	if (encoding < 0)
		encoding = PG_SQL_ASCII;

#else

	/*
	 * On Windows, strftime_win32() always returns UTF8 data, so convert from
	 * that if necessary.
	 */
	encoding = PG_UTF8;

#endif							/* WIN32 */

	bufptr = buf;

	/* localized days */
	for (i = 0; i < 7; i++)
	{
		cache_single_string(&localized_abbrev_days[i], bufptr, encoding);
		bufptr += MAX_L10N_DATA;
		cache_single_string(&localized_full_days[i], bufptr, encoding);
		bufptr += MAX_L10N_DATA;
	}
	localized_abbrev_days[7] = NULL;
	localized_full_days[7] = NULL;

	/* localized months */
	for (i = 0; i < 12; i++)
	{
		cache_single_string(&localized_abbrev_months[i], bufptr, encoding);
		bufptr += MAX_L10N_DATA;
		cache_single_string(&localized_full_months[i], bufptr, encoding);
		bufptr += MAX_L10N_DATA;
	}
	localized_abbrev_months[12] = NULL;
	localized_full_months[12] = NULL;

	CurrentLCTimeValid = true;
}


#if defined(WIN32) && defined(LC_MESSAGES)
/*
 * Convert a Windows setlocale() argument to a Unix-style one.
 *
 * Regardless of platform, we install message catalogs under a Unix-style
 * LL[_CC][.ENCODING][@VARIANT] naming convention.  Only LC_MESSAGES settings
 * following that style will elicit localized interface strings.
 *
 * Before Visual Studio 2012 (msvcr110.dll), Windows setlocale() accepted "C"
 * (but not "c") and strings of the form <Language>[_<Country>][.<CodePage>],
 * case-insensitive.  setlocale() returns the fully-qualified form; for
 * example, setlocale("thaI") returns "Thai_Thailand.874".  Internally,
 * setlocale() and _create_locale() select a "locale identifier"[1] and store
 * it in an undocumented _locale_t field.  From that LCID, we can retrieve the
 * ISO 639 language and the ISO 3166 country.  Character encoding does not
 * matter, because the server and client encodings govern that.
 *
 * Windows Vista introduced the "locale name" concept[2], closely following
 * RFC 4646.  Locale identifiers are now deprecated.  Starting with Visual
 * Studio 2012, setlocale() accepts locale names in addition to the strings it
 * accepted historically.  It does not standardize them; setlocale("Th-tH")
 * returns "Th-tH".  setlocale(category, "") still returns a traditional
 * string.  Furthermore, msvcr110.dll changed the undocumented _locale_t
 * content to carry locale names instead of locale identifiers.
 *
 * Visual Studio 2015 should still be able to do the same as Visual Studio
 * 2012, but the declaration of locale_name is missing in _locale_t, causing
 * this code compilation to fail, hence this falls back instead on to
 * enumerating all system locales by using EnumSystemLocalesEx to find the
 * required locale name.  If the input argument is in Unix-style then we can
 * get ISO Locale name directly by using GetLocaleInfoEx() with LCType as
 * LOCALE_SNAME.
 *
 * This function returns a pointer to a static buffer bearing the converted
 * name or NULL if conversion fails.
 *
 * [1] https://docs.microsoft.com/en-us/windows/win32/intl/locale-identifiers
 * [2] https://docs.microsoft.com/en-us/windows/win32/intl/locale-names
 */

/*
 * Callback function for EnumSystemLocalesEx() in get_iso_localename().
 *
 * This function enumerates all system locales, searching for one that matches
 * an input with the format: <Language>[_<Country>], e.g.
 * English[_United States]
 *
 * The input is a three wchar_t array as an LPARAM. The first element is the
 * locale_name we want to match, the second element is an allocated buffer
 * where the Unix-style locale is copied if a match is found, and the third
 * element is the search status, 1 if a match was found, 0 otherwise.
 */
static BOOL CALLBACK
search_locale_enum(LPWSTR pStr, DWORD dwFlags, LPARAM lparam)
{
	wchar_t		test_locale[LOCALE_NAME_MAX_LENGTH];
	wchar_t   **argv;

	(void) (dwFlags);

	argv = (wchar_t **) lparam;
	*argv[2] = (wchar_t) 0;

	memset(test_locale, 0, sizeof(test_locale));

	/* Get the name of the <Language> in English */
	if (GetLocaleInfoEx(pStr, LOCALE_SENGLISHLANGUAGENAME,
						test_locale, LOCALE_NAME_MAX_LENGTH))
	{
		/*
		 * If the enumerated locale does not have a hyphen ("en") OR the
		 * locale_name input does not have an underscore ("English"), we only
		 * need to compare the <Language> tags.
		 */
		if (wcsrchr(pStr, '-') == NULL || wcsrchr(argv[0], '_') == NULL)
		{
			if (_wcsicmp(argv[0], test_locale) == 0)
			{
				wcscpy(argv[1], pStr);
				*argv[2] = (wchar_t) 1;
				return FALSE;
			}
		}

		/*
		 * We have to compare a full <Language>_<Country> tag, so we append
		 * the underscore and name of the country/region in English, e.g.
		 * "English_United States".
		 */
		else
		{
			size_t		len;

			wcscat(test_locale, L"_");
			len = wcslen(test_locale);
			if (GetLocaleInfoEx(pStr, LOCALE_SENGLISHCOUNTRYNAME,
								test_locale + len,
								LOCALE_NAME_MAX_LENGTH - len))
			{
				if (_wcsicmp(argv[0], test_locale) == 0)
				{
					wcscpy(argv[1], pStr);
					*argv[2] = (wchar_t) 1;
					return FALSE;
				}
			}
		}
	}

	return TRUE;
}

/*
 * This function converts a Windows locale name to an ISO formatted version
 * for Visual Studio 2015 or greater.
 *
 * Returns NULL, if no valid conversion was found.
 */
static char *
get_iso_localename(const char *winlocname)
{
	wchar_t		wc_locale_name[LOCALE_NAME_MAX_LENGTH];
	wchar_t		buffer[LOCALE_NAME_MAX_LENGTH];
	static char iso_lc_messages[LOCALE_NAME_MAX_LENGTH];
	char	   *period;
	int			len;
	int			ret_val;

	/*
	 * Valid locales have the following syntax:
	 * <Language>[_<Country>[.<CodePage>]]
	 *
	 * GetLocaleInfoEx can only take locale name without code-page and for the
	 * purpose of this API the code-page doesn't matter.
	 */
	period = strchr(winlocname, '.');
	if (period != NULL)
		len = period - winlocname;
	else
		len = pg_mbstrlen(winlocname);

	memset(wc_locale_name, 0, sizeof(wc_locale_name));
	memset(buffer, 0, sizeof(buffer));
	MultiByteToWideChar(CP_ACP, 0, winlocname, len, wc_locale_name,
						LOCALE_NAME_MAX_LENGTH);

	/*
	 * If the lc_messages is already a Unix-style string, we have a direct
	 * match with LOCALE_SNAME, e.g. en-US, en_US.
	 */
	ret_val = GetLocaleInfoEx(wc_locale_name, LOCALE_SNAME, (LPWSTR) &buffer,
							  LOCALE_NAME_MAX_LENGTH);
	if (!ret_val)
	{
		/*
		 * Search for a locale in the system that matches language and country
		 * name.
		 */
		wchar_t    *argv[3];

		argv[0] = wc_locale_name;
		argv[1] = buffer;
		argv[2] = (wchar_t *) &ret_val;
		EnumSystemLocalesEx(search_locale_enum, LOCALE_WINDOWS, (LPARAM) argv,
							NULL);
	}

	if (ret_val)
	{
		size_t		rc;
		char	   *hyphen;

		/* Locale names use only ASCII, any conversion locale suffices. */
		rc = wchar2char(iso_lc_messages, buffer, sizeof(iso_lc_messages), NULL);
		if (rc == -1 || rc == sizeof(iso_lc_messages))
			return NULL;

		/*
		 * Since the message catalogs sit on a case-insensitive filesystem, we
		 * need not standardize letter case here.  So long as we do not ship
		 * message catalogs for which it would matter, we also need not
		 * translate the script/variant portion, e.g.  uz-Cyrl-UZ to
		 * uz_UZ@cyrillic.  Simply replace the hyphen with an underscore.
		 */
		hyphen = strchr(iso_lc_messages, '-');
		if (hyphen)
			*hyphen = '_';
		return iso_lc_messages;
	}

	return NULL;
}

static char *
IsoLocaleName(const char *winlocname)
{
	static char iso_lc_messages[LOCALE_NAME_MAX_LENGTH];

	if (pg_strcasecmp("c", winlocname) == 0 ||
		pg_strcasecmp("posix", winlocname) == 0)
	{
		strcpy(iso_lc_messages, "C");
		return iso_lc_messages;
	}
	else
		return get_iso_localename(winlocname);
}

<<<<<<< HEAD
#else							/* !defined(_MSC_VER) */

static char *
IsoLocaleName(const char *winlocname)
{
	char		buf[32];
	const int	canary = 0x7F;
	bool		ok = true;

	/*
	 * Given a two-byte ASCII string and length limit 7, 8 or 9, Solaris 10
	 * 05/08 returns 18 and modifies 10 bytes.  It respects limits above or
	 * below that range.
	 *
	 * The bug is present in Solaris 8 as well; it is absent in Solaris 10
	 * 01/13 and Solaris 11.2.  Affected locales include is_IS.ISO8859-1,
	 * en_US.UTF-8, en_US.ISO8859-1, and ru_RU.KOI8-R.  Unaffected locales
	 * include de_DE.UTF-8, de_DE.ISO8859-1, zh_TW.UTF-8, and C.
	 */
	buf[7] = canary;
	(void) strxfrm(buf, "ab", 7);
	if (buf[7] != canary)
		ok = false;

	/*
	 * illumos bug #1594 was present in the source tree from 2010-10-11 to
	 * 2012-02-01.  Given an ASCII string of any length and length limit 1,
	 * affected systems ignore the length limit and modify a number of bytes
	 * one less than the return value.  The problem inputs for this bug do not
	 * overlap those for the Solaris bug, hence a distinct test.
	 *
	 * Affected systems include smartos-20110926T021612Z.  Affected locales
	 * include en_US.ISO8859-1 and en_US.UTF-8.  Unaffected locales include C.
	 */
	buf[1] = canary;
	(void) strxfrm(buf, "a", 1);
	if (buf[1] != canary)
		ok = false;

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg_internal("strxfrm(), in locale \"%s\", writes past the specified array length",
					SETLOCALE(LC_COLLATE, NULL)),
				 errhint("Apply system library package updates.")));
}

#endif							/* defined(_MSC_VER) */

=======
>>>>>>> REL_18_BETA1_branch
#endif							/* WIN32 && LC_MESSAGES */

/*
 * Create a new pg_locale_t struct for the given collation oid.
 */
static pg_locale_t
create_pg_locale(Oid collid, MemoryContext context)
{
	HeapTuple	tp;
	Form_pg_collation collform;
	pg_locale_t result;
	Datum		datum;
	bool		isnull;

	tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for collation %u", collid);
	collform = (Form_pg_collation) GETSTRUCT(tp);

	if (collform->collprovider == COLLPROVIDER_BUILTIN)
		result = create_pg_locale_builtin(collid, context);
	else if (collform->collprovider == COLLPROVIDER_ICU)
		result = create_pg_locale_icu(collid, context);
	else if (collform->collprovider == COLLPROVIDER_LIBC)
		result = create_pg_locale_libc(collid, context);
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(collform->collprovider);

	result->is_default = false;

	Assert((result->collate_is_c && result->collate == NULL) ||
		   (!result->collate_is_c && result->collate != NULL));

	datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collversion,
							&isnull);
	if (!isnull)
	{
		char	   *actual_versionstr;
		char	   *collversionstr;

		collversionstr = TextDatumGetCString(datum);

		if (collform->collprovider == COLLPROVIDER_LIBC)
			datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_collcollate);
		else
			datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_colllocale);

		actual_versionstr = get_collation_actual_version(collform->collprovider,
														 TextDatumGetCString(datum));
		if (!actual_versionstr)
		{
			/*
			 * This could happen when specifying a version in CREATE COLLATION
			 * but the provider does not support versioning, or manually
			 * creating a mess in the catalogs.
			 */
			ereport(ERROR,
					(errmsg("collation \"%s\" has no actual version, but a version was recorded",
							NameStr(collform->collname))));
		}

		if (strcmp(actual_versionstr, collversionstr) != 0)
			ereport(WARNING,
					(errmsg("collation \"%s\" has version mismatch",
							NameStr(collform->collname)),
					 errdetail("The collation in the database was created using version %s, "
							   "but the operating system provides version %s.",
							   collversionstr, actual_versionstr),
					 errhint("Rebuild all objects affected by this collation and run "
							 "ALTER COLLATION %s REFRESH VERSION, "
							 "or build PostgreSQL with the right library version.",
							 quote_qualified_identifier(get_namespace_name(collform->collnamespace),
														NameStr(collform->collname)))));
	}

	ReleaseSysCache(tp);

<<<<<<< HEAD

/*
 * Detect whether collation's LC_COLLATE property is C
 */
bool
lc_collate_is_c(Oid collation)
{
	/*
	 * If we're asked about "collation 0", return false, so that the code will
	 * go into the non-C path and report that the collation is bogus.
	 */
	if (!OidIsValid(collation))
		return false;

	/*
	 * If we're asked about the default collation, we have to inquire of the C
	 * library.  Cache the result so we only have to compute it once.
	 */
	if (collation == DEFAULT_COLLATION_OID)
	{
		static int	result = -1;
		char	   *localeptr;

		if (default_locale.provider == COLLPROVIDER_ICU)
			return false;

		if (result >= 0)
			return (bool) result;
		localeptr = SETLOCALE(LC_COLLATE, NULL);
		if (!localeptr)
			elog(ERROR, "invalid LC_COLLATE setting");

		if (strcmp(localeptr, "C") == 0)
			result = true;
		else if (strcmp(localeptr, "POSIX") == 0)
			result = true;
		else
			result = false;
		return (bool) result;
	}

	/*
	 * If we're asked about the built-in C/POSIX collations, we know that.
	 */
	if (collation == C_COLLATION_OID ||
		collation == POSIX_COLLATION_OID)
		return true;

	/*
	 * Otherwise, we have to consult pg_collation, but we cache that.
	 */
	return (lookup_collation_cache(collation, true))->collate_is_c;
=======
	return result;
>>>>>>> REL_18_BETA1_branch
}

/*
 * Initialize default_locale with database locale settings.
 */
<<<<<<< HEAD
bool
lc_ctype_is_c(Oid collation)
{
	/*
	 * If we're asked about "collation 0", return false, so that the code will
	 * go into the non-C path and report that the collation is bogus.
	 */
	if (!OidIsValid(collation))
		return false;

	/*
	 * If we're asked about the default collation, we have to inquire of the C
	 * library.  Cache the result so we only have to compute it once.
	 */
	if (collation == DEFAULT_COLLATION_OID)
	{
		static int	result = -1;
		char	   *localeptr;

		if (default_locale.provider == COLLPROVIDER_ICU)
			return false;

		if (result >= 0)
			return (bool) result;
		localeptr = SETLOCALE(LC_CTYPE, NULL);
		if (!localeptr)
			elog(ERROR, "invalid LC_CTYPE setting");

		if (strcmp(localeptr, "C") == 0)
			result = true;
		else if (strcmp(localeptr, "POSIX") == 0)
			result = true;
		else
			result = false;
		return (bool) result;
	}

	/*
	 * If we're asked about the built-in C/POSIX collations, we know that.
	 */
	if (collation == C_COLLATION_OID ||
		collation == POSIX_COLLATION_OID)
		return true;

	/*
	 * Otherwise, we have to consult pg_collation, but we cache that.
	 */
	return (lookup_collation_cache(collation, true))->ctype_is_c;
}

struct pg_locale_struct default_locale;

=======
>>>>>>> REL_18_BETA1_branch
void
init_database_collation(void)
{
	HeapTuple	tup;
	Form_pg_database dbform;
	pg_locale_t result;

	Assert(default_locale == NULL);

	/* Fetch our pg_database row normally, via syscache */
	tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);
	dbform = (Form_pg_database) GETSTRUCT(tup);

	if (dbform->datlocprovider == COLLPROVIDER_BUILTIN)
		result = create_pg_locale_builtin(DEFAULT_COLLATION_OID,
										  TopMemoryContext);
	else if (dbform->datlocprovider == COLLPROVIDER_ICU)
		result = create_pg_locale_icu(DEFAULT_COLLATION_OID,
									  TopMemoryContext);
	else if (dbform->datlocprovider == COLLPROVIDER_LIBC)
		result = create_pg_locale_libc(DEFAULT_COLLATION_OID,
									   TopMemoryContext);
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(dbform->datlocprovider);

	result->is_default = true;
	ReleaseSysCache(tup);

	default_locale = result;
}

/*
 * Create a pg_locale_t from a collation OID.  Results are cached for the
 * lifetime of the backend.  Thus, do not free the result with freelocale().
 *
 * For simplicity, we always generate COLLATE + CTYPE even though we
 * might only need one of them.  Since this is called only once per session,
 * it shouldn't cost much.
 */
pg_locale_t
pg_newlocale_from_collation(Oid collid)
{
	collation_cache_entry *cache_entry;
	bool		found;

	if (collid == DEFAULT_COLLATION_OID)
		return default_locale;

	if (!OidIsValid(collid))
		elog(ERROR, "cache lookup failed for collation %u", collid);

	AssertCouldGetRelation();

	if (last_collation_cache_oid == collid)
		return last_collation_cache_locale;

	if (CollationCache == NULL)
	{
		CollationCacheContext = AllocSetContextCreate(TopMemoryContext,
													  "collation cache",
													  ALLOCSET_DEFAULT_SIZES);
		CollationCache = collation_cache_create(CollationCacheContext,
												16, NULL);
	}

	cache_entry = collation_cache_insert(CollationCache, collid, &found);
	if (!found)
	{
		/*
		 * Make sure cache entry is marked invalid, in case we fail before
		 * setting things.
		 */
		cache_entry->locale = 0;
	}

#ifdef FAULT_INJECTOR
	SIMPLE_FAULT_INJECTOR("collate_locale_os_lookup");
#endif

	if (cache_entry->locale == 0)
	{
<<<<<<< HEAD
		/* We haven't computed this yet in this session, so do it */
		HeapTuple	tp;
		Form_pg_collation collform;
		struct pg_locale_struct result;
		pg_locale_t resultp;
		Datum		datum;
		bool		isnull;

		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", collid);
		collform = (Form_pg_collation) GETSTRUCT(tp);

		/* We'll fill in the result struct locally before allocating memory */
		memset(&result, 0, sizeof(result));
		result.provider = collform->collprovider;
		result.deterministic = collform->collisdeterministic;

		if (collform->collprovider == COLLPROVIDER_LIBC)
		{
#ifdef HAVE_LOCALE_T
			const char *collcollate;
			const char *collctype pg_attribute_unused();
			locale_t	loc;

			datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_collcollate);
			collcollate = TextDatumGetCString(datum);
			datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_collctype);
			collctype = TextDatumGetCString(datum);

			if (strcmp(collcollate, collctype) == 0)
			{
				/* Normal case where they're the same */
				errno = 0;
#ifndef WIN32

				loc = NEWLOCALE(LC_COLLATE_MASK | LC_CTYPE_MASK, collcollate,
								NULL);

#else
				loc = _create_locale(LC_ALL, collcollate);
#endif
				if (!loc)
					report_newlocale_failure(collcollate);
			}
			else
			{
#ifndef WIN32
				/* We need two newlocale() steps */
				locale_t	loc1;

				errno = 0;
				loc1 = NEWLOCALE(LC_COLLATE_MASK, collcollate, NULL);
				if (!loc1)
					report_newlocale_failure(collcollate);
				errno = 0;
				loc = NEWLOCALE(LC_CTYPE_MASK, collctype, loc1);
				if (!loc)
					report_newlocale_failure(collctype);
#else

				/*
				 * XXX The _create_locale() API doesn't appear to support
				 * this. Could perhaps be worked around by changing
				 * pg_locale_t to contain two separate fields.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("collations with different collate and ctype values are not supported on this platform")));
#endif
			}

			result.info.lt = loc;
#else							/* not HAVE_LOCALE_T */
			/* platform that doesn't support locale_t */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("collation provider LIBC is not supported on this platform")));
#endif							/* not HAVE_LOCALE_T */
		}
		else if (collform->collprovider == COLLPROVIDER_ICU)
		{
			const char *iculocstr;
			const char *icurules;

			datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_colliculocale);
			iculocstr = TextDatumGetCString(datum);

			datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collicurules, &isnull);
			if (!isnull)
				icurules = TextDatumGetCString(datum);
			else
				icurules = NULL;

			make_icu_collator(iculocstr, icurules, &result);
		}

		datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collversion,
								&isnull);
		if (!isnull)
		{
			char	   *actual_versionstr;
			char	   *collversionstr;

			collversionstr = TextDatumGetCString(datum);

			datum = SysCacheGetAttrNotNull(COLLOID, tp, collform->collprovider == COLLPROVIDER_ICU ? Anum_pg_collation_colliculocale : Anum_pg_collation_collcollate);

			actual_versionstr = get_collation_actual_version(collform->collprovider,
															 TextDatumGetCString(datum));
			if (!actual_versionstr)
			{
				/*
				 * This could happen when specifying a version in CREATE
				 * COLLATION but the provider does not support versioning, or
				 * manually creating a mess in the catalogs.
				 */
				ereport(ERROR,
						(errmsg("collation \"%s\" has no actual version, but a version was recorded",
								NameStr(collform->collname))));
			}

			if (strcmp(actual_versionstr, collversionstr) != 0)
				ereport(WARNING,
						(errmsg("collation \"%s\" has version mismatch",
								NameStr(collform->collname)),
						 errdetail("The collation in the database was created using version %s, "
								   "but the operating system provides version %s.",
								   collversionstr, actual_versionstr),
						 errhint("Rebuild all objects affected by this collation and run "
								 "ALTER COLLATION %s REFRESH VERSION, "
								 "or build PostgreSQL with the right library version.",
								 quote_qualified_identifier(get_namespace_name(collform->collnamespace),
															NameStr(collform->collname)))));
		}

		ReleaseSysCache(tp);

		/* We'll keep the pg_locale_t structures in TopMemoryContext */
		resultp = MemoryContextAlloc(TopMemoryContext, sizeof(*resultp));
		*resultp = result;

		cache_entry->locale = resultp;
=======
		cache_entry->locale = create_pg_locale(collid, CollationCacheContext);
>>>>>>> REL_18_BETA1_branch
	}

	last_collation_cache_oid = collid;
	last_collation_cache_locale = cache_entry->locale;

	return cache_entry->locale;
}

/*
 * Get provider-specific collation version string for the given collation from
 * the operating system/library.
 */
char *
get_collation_actual_version(char collprovider, const char *collcollate)
{
	char	   *collversion = NULL;

	if (collprovider == COLLPROVIDER_BUILTIN)
		collversion = get_collation_actual_version_builtin(collcollate);
#ifdef USE_ICU
	else if (collprovider == COLLPROVIDER_ICU)
		collversion = get_collation_actual_version_icu(collcollate);
#endif
<<<<<<< HEAD
		if (collprovider == COLLPROVIDER_LIBC &&
			pg_strcasecmp("C", collcollate) != 0 &&
			pg_strncasecmp("C.", collcollate, 2) != 0 &&
			pg_strcasecmp("POSIX", collcollate) != 0)
	{
#if defined(__GLIBC__)
		/* Use the glibc version because we don't have anything better. */
#ifdef USE_MDBLOCALES
		collversion = pstrdup(mdb_localesversion());
#else
		collversion = pstrdup(gnu_get_libc_version());
#endif
#elif defined(LC_VERSION_MASK)
		locale_t	loc;

		/* Look up FreeBSD collation version. */
		loc = NEWLOCALE(LC_COLLATE, collcollate, NULL);
		if (loc)
		{
			collversion =
				pstrdup(querylocale(LC_COLLATE_MASK | LC_VERSION_MASK, loc));
			freelocale(loc);
		}
		else
			ereport(ERROR,
					(errmsg("could not load locale \"%s\"", collcollate)));
#elif defined(WIN32)
		/*
		 * If we are targeting Windows Vista and above, we can ask for a name
		 * given a collation name (earlier versions required a location code
		 * that we don't have).
		 */
		NLSVERSIONINFOEX version = {sizeof(NLSVERSIONINFOEX)};
		WCHAR		wide_collcollate[LOCALE_NAME_MAX_LENGTH];

		MultiByteToWideChar(CP_ACP, 0, collcollate, -1, wide_collcollate,
							LOCALE_NAME_MAX_LENGTH);
		if (!GetNLSVersionEx(COMPARE_STRING, wide_collcollate, &version))
		{
			/*
			 * GetNLSVersionEx() wants a language tag such as "en-US", not a
			 * locale name like "English_United States.1252".  Until those
			 * values can be prevented from entering the system, or 100%
			 * reliably converted to the more useful tag format, tolerate the
			 * resulting error and report that we have no version data.
			 */
			if (GetLastError() == ERROR_INVALID_PARAMETER)
				return NULL;

			ereport(ERROR,
					(errmsg("could not get collation version for locale \"%s\": error code %lu",
							collcollate,
							GetLastError())));
		}
		collversion = psprintf("%lu.%lu,%lu.%lu",
							   (version.dwNLSVersion >> 8) & 0xFFFF,
							   version.dwNLSVersion & 0xFF,
							   (version.dwDefinedVersion >> 8) & 0xFFFF,
							   version.dwDefinedVersion & 0xFF);
#endif
	}
=======
	else if (collprovider == COLLPROVIDER_LIBC)
		collversion = get_collation_actual_version_libc(collcollate);
>>>>>>> REL_18_BETA1_branch

	return collversion;
}

size_t
pg_strlower(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
<<<<<<< HEAD
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	char	   *a1p,
			   *a2p;
	size_t		a1len,
				a2len,
				buflen;
	int			r;
	int			result;

	Assert(!locale || locale->provider == COLLPROVIDER_LIBC);
	Assert(GetDatabaseEncoding() == PG_UTF8);
#ifndef WIN32
	Assert(false);
#endif

	/*
	 * In a 32-bit build, twice the input length can overflow size_t, so we
	 * must be careful.
	 */
	a1len = add_size(add_size(len1, len1), 2);
	a2len = add_size(add_size(len2, len2), 2);
	buflen = add_size(a1len, a2len);

	if (buflen > TEXTBUFLEN)
		buf = palloc(buflen);

	a1p = buf;
	a2p = buf + a1len;

	/* API does not work for zero-length input */
	if (len1 == 0)
		r = 0;
	else
	{
		r = MultiByteToWideChar(CP_UTF8, 0, arg1, len1,
								(LPWSTR) a1p, a1len / 2);
		if (!r)
			ereport(ERROR,
					(errmsg("could not convert string to UTF-16: error code %lu",
							GetLastError())));
	}
	((LPWSTR) a1p)[r] = 0;

	if (len2 == 0)
		r = 0;
	else
	{
		r = MultiByteToWideChar(CP_UTF8, 0, arg2, len2,
								(LPWSTR) a2p, a2len / 2);
		if (!r)
			ereport(ERROR,
					(errmsg("could not convert string to UTF-16: error code %lu",
							GetLastError())));
	}
	((LPWSTR) a2p)[r] = 0;

	errno = 0;
#ifdef HAVE_LOCALE_T
	if (locale)
		result = wcscoll_l((LPWSTR) a1p, (LPWSTR) a2p, locale->info.lt);
	else
#endif
		result = wcscoll((LPWSTR) a1p, (LPWSTR) a2p);
	if (result == 2147483647)	/* _NLSCMPERROR; missing from mingw headers */
		ereport(ERROR,
				(errmsg("could not compare Unicode strings: %m")));

	if (buf != sbuf)
		pfree(buf);

	return result;
}
#endif							/* WIN32 */

/*
 * pg_strcoll_libc
 *
 * Call strcoll(), strcoll_l(), wcscoll(), or wcscoll_l() as appropriate for
 * the given locale, platform, and database encoding. If the locale is NULL,
 * use the database collation.
 *
 * Arguments must be encoded in the database encoding and nul-terminated.
 */
static int
pg_strcoll_libc(const char *arg1, const char *arg2, pg_locale_t locale)
{
	int			result;

	Assert(!locale || locale->provider == COLLPROVIDER_LIBC);
#ifdef WIN32
	if (GetDatabaseEncoding() == PG_UTF8)
	{
		size_t		len1 = strlen(arg1);
		size_t		len2 = strlen(arg2);

		result = pg_strncoll_libc_win32_utf8(arg1, len1, arg2, len2, locale);
	}
	else
#endif							/* WIN32 */
	if (locale)
	{
#ifdef HAVE_LOCALE_T
		result = strcoll_l(arg1, arg2, locale->info.lt);
#else
		/* shouldn't happen */
		elog(ERROR, "unsupported collprovider: %c", locale->provider);
#endif
	}
	else
		result = strcoll(arg1, arg2);

	return result;
}

/*
 * pg_strncoll_libc
 *
 * Nul-terminate the arguments and call pg_strcoll_libc().
 */
static int
pg_strncoll_libc(const char *arg1, size_t len1, const char *arg2, size_t len2,
				 pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	size_t		bufsize1 = len1 + 1;
	size_t		bufsize2 = len2 + 1;
	char	   *arg1n;
	char	   *arg2n;
	int			result;

	Assert(!locale || locale->provider == COLLPROVIDER_LIBC);

#ifdef WIN32
	/* check for this case before doing the work for nul-termination */
	if (GetDatabaseEncoding() == PG_UTF8)
		return pg_strncoll_libc_win32_utf8(arg1, len1, arg2, len2, locale);
#endif							/* WIN32 */

	if (bufsize1 + bufsize2 > TEXTBUFLEN)
		buf = palloc(bufsize1 + bufsize2);

	arg1n = buf;
	arg2n = buf + bufsize1;

	/* nul-terminate arguments */
	memcpy(arg1n, arg1, len1);
	arg1n[len1] = '\0';
	memcpy(arg2n, arg2, len2);
	arg2n[len2] = '\0';

	result = pg_strcoll_libc(arg1n, arg2n, locale);

	if (buf != sbuf)
		pfree(buf);

	return result;
}

#ifdef USE_ICU

/*
 * pg_strncoll_icu_no_utf8
 *
 * Convert the arguments from the database encoding to UChar strings, then
 * call ucol_strcoll(). An argument length of -1 means that the string is
 * NUL-terminated.
 *
 * When the database encoding is UTF-8, and ICU supports ucol_strcollUTF8(),
 * caller should call that instead.
 */
static int
pg_strncoll_icu_no_utf8(const char *arg1, int32_t len1,
						const char *arg2, int32_t len2, pg_locale_t locale)
{
	UChar		sbuf[TEXTBUFLEN / sizeof(UChar)];
	UChar	   *buf = sbuf;
	int32_t		ulen1;
	int32_t		ulen2;
	size_t		bufsize;
	UChar	   *uchar1,
			   *uchar2;
	int			result;

	Assert(locale->provider == COLLPROVIDER_ICU);
#ifdef HAVE_UCOL_STRCOLLUTF8
	Assert(GetDatabaseEncoding() != PG_UTF8);
#endif

	init_icu_converter();

	ulen1 = uchar_length(icu_converter, arg1, len1);
	ulen2 = uchar_length(icu_converter, arg2, len2);

	/* ulen1+1 or ulen2+1 doesn't risk overflow, but summing them might */
	bufsize = add_size(ulen1 + 1, ulen2 + 1);
	if (bufsize > lengthof(sbuf))
		buf = palloc_array(UChar, bufsize);

	uchar1 = buf;
	uchar2 = buf + ulen1 + 1;

	ulen1 = uchar_convert(icu_converter, uchar1, ulen1 + 1, arg1, len1);
	ulen2 = uchar_convert(icu_converter, uchar2, ulen2 + 1, arg2, len2);

	result = ucol_strcoll(locale->info.icu.ucol,
						  uchar1, ulen1,
						  uchar2, ulen2);

	if (buf != sbuf)
		pfree(buf);

	return result;
}

/*
 * pg_strncoll_icu
 *
 * Call ucol_strcollUTF8() or ucol_strcoll() as appropriate for the given
 * database encoding. An argument length of -1 means the string is
 * NUL-terminated.
 *
 * Arguments must be encoded in the database encoding.
 */
static int
pg_strncoll_icu(const char *arg1, int32_t len1, const char *arg2, int32_t len2,
				pg_locale_t locale)
{
	int			result;

	Assert(locale->provider == COLLPROVIDER_ICU);

#ifdef HAVE_UCOL_STRCOLLUTF8
	if (GetDatabaseEncoding() == PG_UTF8)
	{
		UErrorCode	status;

		status = U_ZERO_ERROR;
		result = ucol_strcollUTF8(locale->info.icu.ucol,
								  arg1, len1,
								  arg2, len2,
								  &status);
		if (U_FAILURE(status))
			ereport(ERROR,
					(errmsg("collation failed: %s", u_errorName(status))));
	}
=======
	if (locale->provider == COLLPROVIDER_BUILTIN)
		return strlower_builtin(dst, dstsize, src, srclen, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		return strlower_icu(dst, dstsize, src, srclen, locale);
#endif
	else if (locale->provider == COLLPROVIDER_LIBC)
		return strlower_libc(dst, dstsize, src, srclen, locale);
>>>>>>> REL_18_BETA1_branch
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return 0;					/* keep compiler quiet */
}

size_t
pg_strtitle(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	if (locale->provider == COLLPROVIDER_BUILTIN)
		return strtitle_builtin(dst, dstsize, src, srclen, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		return strtitle_icu(dst, dstsize, src, srclen, locale);
#endif
	else if (locale->provider == COLLPROVIDER_LIBC)
		return strtitle_libc(dst, dstsize, src, srclen, locale);
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return 0;					/* keep compiler quiet */
}

size_t
pg_strupper(char *dst, size_t dstsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	if (locale->provider == COLLPROVIDER_BUILTIN)
		return strupper_builtin(dst, dstsize, src, srclen, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		return strupper_icu(dst, dstsize, src, srclen, locale);
#endif
	else if (locale->provider == COLLPROVIDER_LIBC)
		return strupper_libc(dst, dstsize, src, srclen, locale);
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return 0;					/* keep compiler quiet */
}

size_t
pg_strfold(char *dst, size_t dstsize, const char *src, ssize_t srclen,
		   pg_locale_t locale)
{
	if (locale->provider == COLLPROVIDER_BUILTIN)
		return strfold_builtin(dst, dstsize, src, srclen, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		return strfold_icu(dst, dstsize, src, srclen, locale);
#endif
	/* for libc, just use strlower */
	else if (locale->provider == COLLPROVIDER_LIBC)
		return strlower_libc(dst, dstsize, src, srclen, locale);
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return 0;					/* keep compiler quiet */
}

/*
 * pg_strcoll
 *
 * Like pg_strncoll for NUL-terminated input strings.
 */
int
pg_strcoll(const char *arg1, const char *arg2, pg_locale_t locale)
{
<<<<<<< HEAD
	int			result;

	if (!locale || locale->provider == COLLPROVIDER_LIBC)
		result = pg_strcoll_libc(arg1, arg2, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		result = pg_strncoll_icu(arg1, -1, arg2, -1, locale);
#endif
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return result;
=======
	return locale->collate->strncoll(arg1, -1, arg2, -1, locale);
>>>>>>> REL_18_BETA1_branch
}

/*
 * pg_strncoll
 *
 * Call ucol_strcollUTF8(), ucol_strcoll(), strcoll_l() or wcscoll_l() as
 * appropriate for the given locale, platform, and database encoding. If the
 * locale is not specified, use the database collation.
 *
 * The input strings must be encoded in the database encoding. If an input
 * string is NUL-terminated, its length may be specified as -1.
 *
 * The caller is responsible for breaking ties if the collation is
 * deterministic; this maintains consistency with pg_strnxfrm(), which cannot
 * easily account for deterministic collations.
 */
int
pg_strncoll(const char *arg1, ssize_t len1, const char *arg2, ssize_t len2,
			pg_locale_t locale)
{
<<<<<<< HEAD
	int			result;

	if (!locale || locale->provider == COLLPROVIDER_LIBC)
		result = pg_strncoll_libc(arg1, len1, arg2, len2, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		result = pg_strncoll_icu(arg1, len1, arg2, len2, locale);
#endif
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return result;
}


static size_t
pg_strxfrm_libc(char *dest, const char *src, size_t destsize,
				pg_locale_t locale)
{
	Assert(!locale || locale->provider == COLLPROVIDER_LIBC);

#ifdef TRUST_STRXFRM
#ifdef HAVE_LOCALE_T
	if (locale)
		return strxfrm_l(dest, src, destsize, locale->info.lt);
	else
#endif
		return strxfrm(dest, src, destsize);
#else
	/* shouldn't happen */
	PGLOCALE_SUPPORT_ERROR(locale->provider);
	return 0;					/* keep compiler quiet */
#endif
}

static size_t
pg_strnxfrm_libc(char *dest, const char *src, size_t srclen, size_t destsize,
				 pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	size_t		bufsize = srclen + 1;
	size_t		result;

	Assert(!locale || locale->provider == COLLPROVIDER_LIBC);

	if (bufsize > TEXTBUFLEN)
		buf = palloc(bufsize);

	/* nul-terminate arguments */
	memcpy(buf, src, srclen);
	buf[srclen] = '\0';

	result = pg_strxfrm_libc(dest, buf, destsize, locale);

	if (buf != sbuf)
		pfree(buf);

	/* if dest is defined, it should be nul-terminated */
	Assert(result >= destsize || dest[result] == '\0');

	return result;
}

#ifdef USE_ICU

/* 'srclen' of -1 means the strings are NUL-terminated */
static size_t
pg_strnxfrm_icu(char *dest, const char *src, int32_t srclen, int32_t destsize,
				pg_locale_t locale)
{
	UChar		sbuf[TEXTBUFLEN / sizeof(UChar)];
	UChar	   *uchar = sbuf;
	int32_t		ulen;
	Size		result_bsize;

	Assert(locale->provider == COLLPROVIDER_ICU);

	init_icu_converter();

	ulen = uchar_length(icu_converter, src, srclen);

	if (ulen >= lengthof(sbuf))
		uchar = palloc_array(UChar, ulen + 1);

	ulen = uchar_convert(icu_converter, uchar, ulen + 1, src, srclen);

	result_bsize = ucol_getSortKey(locale->info.icu.ucol,
								   uchar, ulen,
								   (uint8_t *) dest, destsize);

	/*
	 * ucol_getSortKey() counts the nul-terminator in the result length, but
	 * this function should not.
	 */
	Assert(result_bsize > 0);
	result_bsize--;

	if (uchar != sbuf)
		pfree(uchar);

	/* if dest is defined, it should be nul-terminated */
	Assert(result_bsize >= destsize || dest[result_bsize] == '\0');

	return result_bsize;
}

/* 'srclen' of -1 means the strings are NUL-terminated */
static size_t
pg_strnxfrm_prefix_icu_no_utf8(char *dest, const char *src, int32_t srclen,
							   int32_t destsize, pg_locale_t locale)
{
	UChar		sbuf[TEXTBUFLEN / sizeof(UChar)];
	UChar	   *uchar = sbuf;
	UCharIterator iter;
	uint32_t	state[2];
	UErrorCode	status;
	int32_t		ulen;
	Size		result_bsize;

	Assert(locale->provider == COLLPROVIDER_ICU);
	Assert(GetDatabaseEncoding() != PG_UTF8);

	init_icu_converter();

	ulen = uchar_length(icu_converter, src, srclen);

	if (ulen >= lengthof(sbuf))
		uchar = palloc_array(UChar, ulen + 1);

	ulen = uchar_convert(icu_converter, uchar, ulen + 1, src, srclen);

	uiter_setString(&iter, uchar, ulen);
	state[0] = state[1] = 0;	/* won't need that again */
	status = U_ZERO_ERROR;
	result_bsize = ucol_nextSortKeyPart(locale->info.icu.ucol,
										&iter,
										state,
										(uint8_t *) dest,
										destsize,
										&status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("sort key generation failed: %s",
						u_errorName(status))));

	if (uchar != sbuf)
		pfree(uchar);

	return result_bsize;
}

/* 'srclen' of -1 means the strings are NUL-terminated */
static size_t
pg_strnxfrm_prefix_icu(char *dest, const char *src, int32_t srclen,
					   int32_t destsize, pg_locale_t locale)
{
	size_t		result;

	Assert(locale->provider == COLLPROVIDER_ICU);

	if (GetDatabaseEncoding() == PG_UTF8)
	{
		UCharIterator iter;
		uint32_t	state[2];
		UErrorCode	status;

		uiter_setUTF8(&iter, src, srclen);
		state[0] = state[1] = 0;	/* won't need that again */
		status = U_ZERO_ERROR;
		result = ucol_nextSortKeyPart(locale->info.icu.ucol,
									  &iter,
									  state,
									  (uint8_t *) dest,
									  destsize,
									  &status);
		if (U_FAILURE(status))
			ereport(ERROR,
					(errmsg("sort key generation failed: %s",
							u_errorName(status))));
	}
	else
		result = pg_strnxfrm_prefix_icu_no_utf8(dest, src, srclen, destsize,
												locale);

	return result;
}

#endif

=======
	return locale->collate->strncoll(arg1, len1, arg2, len2, locale);
}

>>>>>>> REL_18_BETA1_branch
/*
 * Return true if the collation provider supports pg_strxfrm() and
 * pg_strnxfrm(); otherwise false.
 *
 *
 * No similar problem is known for the ICU provider.
 */
bool
pg_strxfrm_enabled(pg_locale_t locale)
{
<<<<<<< HEAD
	if (!locale || locale->provider == COLLPROVIDER_LIBC)
#ifdef TRUST_STRXFRM
		return true;
#else
		return false;
#endif
	else if (locale->provider == COLLPROVIDER_ICU)
		return true;
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return false;				/* keep compiler quiet */
=======
	/*
	 * locale->collate->strnxfrm is still a required method, even if it may
	 * have the wrong behavior, because the planner uses it for estimates in
	 * some cases.
	 */
	return locale->collate->strxfrm_is_safe;
>>>>>>> REL_18_BETA1_branch
}

/*
 * pg_strxfrm
 *
<<<<<<< HEAD
 * Transforms 'src' to a nul-terminated string stored in 'dest' such that
 * ordinary strcmp() on transformed strings is equivalent to pg_strcoll() on
 * untransformed strings.
 *
 * The provided 'src' must be nul-terminated. If 'destsize' is zero, 'dest'
 * may be NULL.
 *
 * Returns the number of bytes needed (or more) to store the transformed
 * string, excluding the terminating nul byte. If the value returned is
 * 'destsize' or greater, the resulting contents of 'dest' are undefined.
=======
 * Like pg_strnxfrm for a NUL-terminated input string.
>>>>>>> REL_18_BETA1_branch
 */
size_t
pg_strxfrm(char *dest, const char *src, size_t destsize, pg_locale_t locale)
{
<<<<<<< HEAD
	size_t		result = 0;		/* keep compiler quiet */

	if (!locale || locale->provider == COLLPROVIDER_LIBC)
		result = pg_strxfrm_libc(dest, src, destsize, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		result = pg_strnxfrm_icu(dest, src, -1, destsize, locale);
#endif
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return result;
=======
	return locale->collate->strnxfrm(dest, destsize, src, -1, locale);
>>>>>>> REL_18_BETA1_branch
}

/*
 * pg_strnxfrm
 *
 * Transforms 'src' to a nul-terminated string stored in 'dest' such that
 * ordinary strcmp() on transformed strings is equivalent to pg_strcoll() on
 * untransformed strings.
 *
 * The input string must be encoded in the database encoding. If the input
 * string is NUL-terminated, its length may be specified as -1. If 'destsize'
 * is zero, 'dest' may be NULL.
 *
<<<<<<< HEAD
 * Returns the number of bytes needed (or more) to store the transformed
 * string, excluding the terminating nul byte. If the value returned is
 * 'destsize' or greater, the resulting contents of 'dest' are undefined.
=======
 * Not all providers support pg_strnxfrm() safely. The caller should check
 * pg_strxfrm_enabled() first, otherwise this function may return wrong
 * results or an error.
>>>>>>> REL_18_BETA1_branch
 *
 * Returns the number of bytes needed (or more) to store the transformed
 * string, excluding the terminating nul byte. If the value returned is
 * 'destsize' or greater, the resulting contents of 'dest' are undefined.
 */
size_t
pg_strnxfrm(char *dest, size_t destsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
<<<<<<< HEAD
	size_t		result = 0;		/* keep compiler quiet */

	if (!locale || locale->provider == COLLPROVIDER_LIBC)
		result = pg_strnxfrm_libc(dest, src, srclen, destsize, locale);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		result = pg_strnxfrm_icu(dest, src, srclen, destsize, locale);
#endif
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return result;
=======
	return locale->collate->strnxfrm(dest, destsize, src, srclen, locale);
>>>>>>> REL_18_BETA1_branch
}

/*
 * Return true if the collation provider supports pg_strxfrm_prefix() and
 * pg_strnxfrm_prefix(); otherwise false.
 */
bool
pg_strxfrm_prefix_enabled(pg_locale_t locale)
{
<<<<<<< HEAD
	if (!locale || locale->provider == COLLPROVIDER_LIBC)
		return false;
	else if (locale->provider == COLLPROVIDER_ICU)
		return true;
	else
		/* shouldn't happen */
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return false;				/* keep compiler quiet */
=======
	return (locale->collate->strnxfrm_prefix != NULL);
>>>>>>> REL_18_BETA1_branch
}

/*
 * pg_strxfrm_prefix
 *
 * Like pg_strnxfrm_prefix for a NUL-terminated input string.
 */
size_t
pg_strxfrm_prefix(char *dest, const char *src, size_t destsize,
				  pg_locale_t locale)
{
<<<<<<< HEAD
	size_t		result = 0;		/* keep compiler quiet */

	if (!locale)
		PGLOCALE_SUPPORT_ERROR(COLLPROVIDER_LIBC);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		result = pg_strnxfrm_prefix_icu(dest, src, -1, destsize, locale);
#endif
	else
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return result;
=======
	return locale->collate->strnxfrm_prefix(dest, destsize, src, -1, locale);
>>>>>>> REL_18_BETA1_branch
}

/*
 * pg_strnxfrm_prefix
 *
 * Transforms 'src' to a byte sequence stored in 'dest' such that ordinary
 * memcmp() on the byte sequence is equivalent to pg_strncoll() on
 * untransformed strings. The result is not nul-terminated.
 *
 * The input string must be encoded in the database encoding. If the input
 * string is NUL-terminated, its length may be specified as -1.
 *
 * Not all providers support pg_strnxfrm_prefix() safely. The caller should
 * check pg_strxfrm_prefix_enabled() first, otherwise this function may return
 * wrong results or an error.
 *
 * If destsize is not large enough to hold the resulting byte sequence, stores
 * only the first destsize bytes in 'dest'. Returns the number of bytes
 * actually copied to 'dest'.
 */
size_t
pg_strnxfrm_prefix(char *dest, size_t destsize, const char *src,
				   ssize_t srclen, pg_locale_t locale)
{
<<<<<<< HEAD
	size_t		result = 0;		/* keep compiler quiet */

	if (!locale)
		PGLOCALE_SUPPORT_ERROR(COLLPROVIDER_LIBC);
#ifdef USE_ICU
	else if (locale->provider == COLLPROVIDER_ICU)
		result = pg_strnxfrm_prefix_icu(dest, src, -1, destsize, locale);
#endif
	else
		PGLOCALE_SUPPORT_ERROR(locale->provider);

	return result;
}

#ifdef USE_ICU

/*
 * Wrapper around ucol_open() to handle API differences for older ICU
 * versions.
 */
static UCollator *
pg_ucol_open(const char *loc_str)
{
	UCollator  *collator;
	UErrorCode	status;
	const char *orig_str = loc_str;
	char	   *fixed_str = NULL;

	/*
	 * Must never open default collator, because it depends on the environment
	 * and may change at any time. Should not happen, but check here to catch
	 * bugs that might be hard to catch otherwise.
	 *
	 * NB: the default collator is not the same as the collator for the root
	 * locale. The root locale may be specified as the empty string, "und", or
	 * "root". The default collator is opened by passing NULL to ucol_open().
	 */
	if (loc_str == NULL)
		elog(ERROR, "opening default collator is not supported");

	/*
	 * In ICU versions 54 and earlier, "und" is not a recognized spelling of
	 * the root locale. If the first component of the locale is "und", replace
	 * with "root" before opening.
	 */
	if (U_ICU_VERSION_MAJOR_NUM < 55)
	{
		char		lang[ULOC_LANG_CAPACITY];

		status = U_ZERO_ERROR;
		uloc_getLanguage(loc_str, lang, ULOC_LANG_CAPACITY, &status);
		if (U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING)
		{
			ereport(ERROR,
					(errmsg("could not get language from locale \"%s\": %s",
							loc_str, u_errorName(status))));
		}

		if (strcmp(lang, "und") == 0)
		{
			const char *remainder = loc_str + strlen("und");

			fixed_str = palloc(strlen("root") + strlen(remainder) + 1);
			strcpy(fixed_str, "root");
			strcat(fixed_str, remainder);

			loc_str = fixed_str;
		}
	}

	status = U_ZERO_ERROR;
	collator = ucol_open(loc_str, &status);
	if (U_FAILURE(status))
		ereport(ERROR,
		/* use original string for error report */
				(errmsg("could not open collator for locale \"%s\": %s",
						orig_str, u_errorName(status))));

	if (U_ICU_VERSION_MAJOR_NUM < 54)
	{
		status = U_ZERO_ERROR;
		icu_set_collation_attributes(collator, loc_str, &status);

		/*
		 * Pretend the error came from ucol_open(), for consistent error
		 * message across ICU versions.
		 */
		if (U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING)
		{
			ucol_close(collator);
			ereport(ERROR,
					(errmsg("could not open collator for locale \"%s\": %s",
							orig_str, u_errorName(status))));
		}
	}

	if (fixed_str != NULL)
		pfree(fixed_str);

	return collator;
}

static void
init_icu_converter(void)
{
	const char *icu_encoding_name;
	UErrorCode	status;
	UConverter *conv;

	if (icu_converter)
		return;					/* already done */

	icu_encoding_name = get_encoding_name_for_icu(GetDatabaseEncoding());
	if (!icu_encoding_name)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("encoding \"%s\" not supported by ICU",
						pg_encoding_to_char(GetDatabaseEncoding()))));

	status = U_ZERO_ERROR;
	conv = ucnv_open(icu_encoding_name, &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("could not open ICU converter for encoding \"%s\": %s",
						icu_encoding_name, u_errorName(status))));

	icu_converter = conv;
}

/*
 * Find length, in UChars, of given string if converted to UChar string.
 *
 * Note: given the assumption that the input string fits in MaxAllocSize,
 * the result cannot overflow int32_t.  But callers must be careful about
 * multiplying the result by sizeof(UChar).
 */
static int32_t
uchar_length(UConverter *converter, const char *str, int32_t len)
=======
	return locale->collate->strnxfrm_prefix(dest, destsize, src, srclen, locale);
}

/*
 * Return required encoding ID for the given locale, or -1 if any encoding is
 * valid for the locale.
 */
int
builtin_locale_encoding(const char *locale)
>>>>>>> REL_18_BETA1_branch
{
	if (strcmp(locale, "C") == 0)
		return -1;
	else if (strcmp(locale, "C.UTF-8") == 0)
		return PG_UTF8;
	else if (strcmp(locale, "PG_UNICODE_FAST") == 0)
		return PG_UTF8;


	ereport(ERROR,
			(errcode(ERRCODE_WRONG_OBJECT_TYPE),
			 errmsg("invalid locale name \"%s\" for builtin provider",
					locale)));

	return 0;					/* keep compiler quiet */
}


/*
 * Validate the locale and encoding combination, and return the canonical form
 * of the locale name.
 */
const char *
builtin_validate_locale(int encoding, const char *locale)
{
	const char *canonical_name = NULL;
	int			required_encoding;

<<<<<<< HEAD
	ulen = ucnv_toUChars(converter, dest, destlen, src, srclen, &status);
	if (U_FAILURE(status))
=======
	if (strcmp(locale, "C") == 0)
		canonical_name = "C";
	else if (strcmp(locale, "C.UTF-8") == 0 || strcmp(locale, "C.UTF8") == 0)
		canonical_name = "C.UTF-8";
	else if (strcmp(locale, "PG_UNICODE_FAST") == 0)
		canonical_name = "PG_UNICODE_FAST";

	if (!canonical_name)
>>>>>>> REL_18_BETA1_branch
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("invalid locale name \"%s\" for builtin provider",
						locale)));

<<<<<<< HEAD
/*
 * Convert a string in the database encoding into a string of UChars.
 *
 * The source string at buff is of length nbytes
 * (it needn't be nul-terminated)
 *
 * *buff_uchar receives a pointer to the palloc'd result string, and
 * the function's result is the number of UChars generated.
 *
 * The result string is nul-terminated, though most callers rely on the
 * result length instead.
 */
int32_t
icu_to_uchar(UChar **buff_uchar, const char *buff, size_t nbytes)
{
	int32_t		len_uchar;

	init_icu_converter();

	len_uchar = uchar_length(icu_converter, buff, nbytes);

	*buff_uchar = palloc_array(UChar, len_uchar + 1);
	len_uchar = uchar_convert(icu_converter,
							  *buff_uchar, len_uchar + 1, buff, nbytes);

	return len_uchar;
}

/*
 * Convert a string of UChars into the database encoding.
 *
 * The source string at buff_uchar is of length len_uchar
 * (it needn't be nul-terminated)
 *
 * *result receives a pointer to the palloc'd result string, and the
 * function's result is the number of bytes generated (not counting nul).
 *
 * The result string is nul-terminated.
 */
int32_t
icu_from_uchar(char **result, const UChar *buff_uchar, int32_t len_uchar)
{
	UErrorCode	status;
	int32_t		len_result;

	init_icu_converter();

	status = U_ZERO_ERROR;
	len_result = ucnv_fromUChars(icu_converter, NULL, 0,
								 buff_uchar, len_uchar, &status);
	if (U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR)
=======
	required_encoding = builtin_locale_encoding(canonical_name);
	if (required_encoding >= 0 && encoding != required_encoding)
>>>>>>> REL_18_BETA1_branch
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("encoding \"%s\" does not match locale \"%s\"",
						pg_encoding_to_char(encoding), locale)));

	return canonical_name;
}



/*
 * Return the BCP47 language tag representation of the requested locale.
 *
 * This function should be called before passing the string to ucol_open(),
 * because conversion to a language tag also performs "level 2
 * canonicalization". In addition to producing a consistent format, level 2
 * canonicalization is able to more accurately interpret different input
 * locale string formats, such as POSIX and .NET IDs.
 */
char *
icu_language_tag(const char *loc_str, int elevel)
{
#ifdef USE_ICU
	UErrorCode	status;
	char	   *langtag;
	size_t		buflen = 32;	/* arbitrary starting buffer size */
	const bool	strict = true;

	/*
	 * A BCP47 language tag doesn't have a clearly-defined upper limit (cf.
	 * RFC5646 section 4.4). Additionally, in older ICU versions,
	 * uloc_toLanguageTag() doesn't always return the ultimate length on the
	 * first call, necessitating a loop.
	 */
	langtag = palloc(buflen);
	while (true)
	{
		status = U_ZERO_ERROR;
		uloc_toLanguageTag(loc_str, langtag, buflen, strict, &status);

		/* try again if the buffer is not large enough */
		if ((status == U_BUFFER_OVERFLOW_ERROR ||
			 status == U_STRING_NOT_TERMINATED_WARNING) &&
			buflen < MaxAllocSize)
		{
			buflen = Min(buflen * 2, MaxAllocSize);
			langtag = repalloc(langtag, buflen);
			continue;
		}

		break;
	}

	if (U_FAILURE(status))
	{
		pfree(langtag);

		if (elevel > 0)
			ereport(elevel,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not convert locale name \"%s\" to language tag: %s",
							loc_str, u_errorName(status))));
		return NULL;
	}

	return langtag;
#else							/* not USE_ICU */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ICU is not supported in this build")));
	return NULL;				/* keep compiler quiet */
#endif							/* not USE_ICU */
}

/*
 * Perform best-effort check that the locale is a valid one.
 */
void
icu_validate_locale(const char *loc_str)
{
#ifdef USE_ICU
	UCollator  *collator;
	UErrorCode	status;
	char		lang[ULOC_LANG_CAPACITY];
	bool		found = false;
	int			elevel = icu_validation_level;

	/* no validation */
	if (elevel < 0)
		return;

	/* downgrade to WARNING during pg_upgrade */
	if (IsBinaryUpgrade && elevel > WARNING)
		elevel = WARNING;

	/* validate that we can extract the language */
	status = U_ZERO_ERROR;
	uloc_getLanguage(loc_str, lang, ULOC_LANG_CAPACITY, &status);
	if (U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING)
	{
		ereport(elevel,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not get language from ICU locale \"%s\": %s",
						loc_str, u_errorName(status)),
				 errhint("To disable ICU locale validation, set the parameter \"%s\" to \"%s\".",
						 "icu_validation_level", "disabled")));
		return;
	}

	/* check for special language name */
	if (strcmp(lang, "") == 0 ||
		strcmp(lang, "root") == 0 || strcmp(lang, "und") == 0)
		found = true;

	/* search for matching language within ICU */
	for (int32_t i = 0; !found && i < uloc_countAvailable(); i++)
	{
		const char *otherloc = uloc_getAvailable(i);
		char		otherlang[ULOC_LANG_CAPACITY];

		status = U_ZERO_ERROR;
		uloc_getLanguage(otherloc, otherlang, ULOC_LANG_CAPACITY, &status);
		if (U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING)
			continue;

		if (strcmp(lang, otherlang) == 0)
			found = true;
	}

	if (!found)
		ereport(elevel,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ICU locale \"%s\" has unknown language \"%s\"",
						loc_str, lang),
				 errhint("To disable ICU locale validation, set the parameter \"%s\" to \"%s\".",
						 "icu_validation_level", "disabled")));

	/* check that it can be opened */
	collator = pg_ucol_open(loc_str);
	ucol_close(collator);
#else							/* not USE_ICU */
	/* could get here if a collation was created by a build with ICU */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ICU is not supported in this build")));
#endif							/* not USE_ICU */
}
