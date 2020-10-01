# CHECK_MAJOR_VERSION(VARIABLE, VERSION, [ACTION-IF-TRUE], [ACTION-IF-FALSE])  
# ---------------------------------------------------------------------------
# Run ACTION-IF-TRUE if the VAR has a major version >= VERSION.
# Run ACTION-IF-FALSE otherwise.
#
# From https://stackoverflow.com/questions/4619664/autofoo-test-for-maximum-version-of-python
AC_DEFUN([CHECK_MAJOR_VERSION],
[AC_MSG_CHECKING([whether $1 $$1 major version == $2])
case $$1 in
$2*)
AC_MSG_RESULT([yes])
ifelse([$3], [$3], [:])
;;
*)
AC_MSG_RESULT([no])
ifelse([$4], , [AC_MSG_ERROR([$$1 differs from $2])], [$4])
;;
esac])
