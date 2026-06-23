# Copyright (c) 2022-2025, PostgreSQL Global Development Group

tests += {
  'name': 'unsafe_tests',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'rolenames',
      'setconfig',
      'alter_system_table',
      'guc_privs',
    ],
<<<<<<< HEAD
    'regress_args': ['--create-role=regress_authenticated_user_sr',
=======
    'regress_args': ['--create-role=regress_authenticated_user_db_sr',
                     '--create-role=regress_authenticated_user_db_ssa',
                     '--create-role=regress_authenticated_user_sr',
>>>>>>> REL_18_BETA1_branch
                     '--create-role=regress_authenticated_user_ssa'],
    'runningcheck': false,
  },
}
