
# Copyright (c) 2021-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

command_exit_is([ 'pg_ctl', 'status', '--pgdata' => "$tempdir/nonexistent" ],
	4, 'pg_ctl status with nonexistent directory');

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;

command_exit_is([ 'pg_ctl', 'status', '--pgdata' => $node->data_dir ],
	3, 'pg_ctl status with server not running');

<<<<<<< HEAD
system_or_bail 'pg_ctl', '-l', "$tempdir/logfile", '-D',
  $node->data_dir, '-w', 'start', '-o', '-c gp_role=utility --gp_dbid=-1 --gp_contentid=-1';
command_exit_is([ 'pg_ctl', 'status', '-D', $node->data_dir ],
=======
system_or_bail(
	'pg_ctl',
	'--log' => "$tempdir/logfile",
	'--pgdata' => $node->data_dir,
	'--wait', 'start');
command_exit_is([ 'pg_ctl', 'status', '--pgdata' => $node->data_dir ],
>>>>>>> REL_18_BETA1_branch
	0, 'pg_ctl status with server running');

system_or_bail 'pg_ctl', 'stop', '--pgdata' => $node->data_dir;

done_testing();
