#!/usr/bin/perl
#----------------------------------------------------------------------
#
# Generate node support files:
# - nodetags.h
# - copyfuncs
# - equalfuncs
# - readfuncs
# - outfuncs
#
# Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/nodes/gen_node_support.pl
#
#----------------------------------------------------------------------

use strict;
use warnings;

use File::Basename;
use Getopt::Long;

use FindBin;
use lib "$FindBin::RealBin/../catalog";

use Catalog;    # for RenameTempFile

my $output_path = '.';

GetOptions('outdir:s' => \$output_path)
  or die "$0: wrong arguments";


# Test whether first argument is element of the list in the second
# argument
sub elem
{
	my $x = shift;
	return grep { $_ eq $x } @_;
}


# This list defines the canonical set of header files to be read by this
# script, and the order they are to be processed in.  We must have a stable
# processing order, else the NodeTag enum's order will vary, with catastrophic
# consequences for ABI stability across different builds.
#
# Currently, the various build systems also have copies of this list,
# so that they can do dependency checking properly.  In future we may be
# able to make this list the only copy.  For now, we just check that
# it matches the list of files passed on the command line.
my @all_input_files = qw(
  catalog/gp_distribution_policy.h
  nodes/nodes.h
  nodes/primnodes.h
  nodes/parsenodes.h
  nodes/pathnodes.h
  nodes/plannodes.h
  nodes/execnodes.h
  access/amapi.h
  access/sdir.h
  access/tableam.h
  access/tsmapi.h
  commands/event_trigger.h
  commands/trigger.h
  executor/tuptable.h
  foreign/fdwapi.h
  nodes/bitmapset.h
  nodes/extensible.h
  nodes/lockoptions.h
  nodes/miscnodes.h
  nodes/replnodes.h
  nodes/supportnodes.h
  nodes/value.h
  utils/rel.h
);

# Nodes from these input files are automatically treated as nodetag_only.
# In the future we might add explicit pg_node_attr labeling to some of these
# files and remove them from this list, but for now this is the path of least
# resistance.
my @nodetag_only_files = qw(
  nodes/execnodes.h
  access/amapi.h
  access/sdir.h
  access/tableam.h
  access/tsmapi.h
  commands/event_trigger.h
  commands/trigger.h
  executor/tuptable.h
  foreign/fdwapi.h
  nodes/lockoptions.h
  nodes/miscnodes.h
  nodes/replnodes.h
  nodes/supportnodes.h
);

# ARM ABI STABILITY CHECK HERE:
#
# In stable branches, set $last_nodetag to the name of the last node type
# that should receive an auto-generated nodetag number, and $last_nodetag_no
# to its number.  (Find these values in the last line of the current
# nodetags.h file.)  The script will then complain if those values don't
# match reality, providing a cross-check that we haven't broken ABI by
# adding or removing nodetags.
# In HEAD, these variables should be left undef, since we don't promise
# ABI stability during development.

my $last_nodetag = 'WindowObjectData';
my $last_nodetag_no = 561;

# output file names
my @output_files;

# collect node names
my @node_types = qw(Node);
# collect info for each node type
my %node_type_info;

# node types we don't want copy support for
my @no_copy;
# node types we don't want equal support for
my @no_equal;
# node types we don't want query jumble support for
my @no_query_jumble;
# node types we don't want read support for
my @no_read;
# node types we don't want read/write support for
my @no_read_write;
# node types that have handmade read/write support
my @special_read_write;
# node types we don't want any support functions for, just node tags
my @nodetag_only;

# types that are copied by straight assignment
my @scalar_types = qw(
  bits32 bool char double int int8 int16 int32 int64 long uint8 uint16 uint32 uint64
  AclMode AttrNumber Cardinality Cost Index Oid RelFileNumber Selectivity Size StrategyNumber SubTransactionId TimeLineID XLogRecPtr
);

# collect enum types
my @enum_types;

# collect types that are abstract (hence no node tag, no support functions)
my @abstract_types = qw(Node);

# Special cases that either don't have their own struct or the struct
# is not in a header file.  We generate node tags for them, but
# they otherwise don't participate in node support.
my @extra_tags = qw(
  IntList OidList XidList
  AllocSetContext GenerationContext SlabContext
  TIDBitmap
  WindowObjectData
);

# This is a regular node, but we skip parsing it from its header file
# since we won't use its internal structure here anyway.
push @node_types, qw(List);
# Lists are specially treated in all five support files, too.
# (Ideally we'd mark List as "special copy/equal" not "no copy/equal".
# But until there's other use-cases for that, just hot-wire the tests
# that would need to distinguish.)
push @no_copy, qw(List);
push @no_equal, qw(List);
push @no_query_jumble, qw(List);
push @special_read_write, qw(List);

# Nodes with custom copy/equal implementations are skipped from
# .funcs.c but need case statements in .switch.c.
my @custom_copy_equal;

# Similarly for custom read/write implementations.
my @custom_read_write;

# Similarly for custom query jumble implementation.
my @custom_query_jumble;

# Track node types with manually assigned NodeTag numbers.
my %manual_nodetag_number;

# This is a struct, so we can copy it by assignment.  Equal support is
# currently not required.
push @scalar_types, qw(QualCost);


## check that we have the expected number of files on the command line
die "wrong number of input files, expected:\n@all_input_files\ngot:\n@ARGV\n"
  if ($#ARGV != $#all_input_files);

## read input

my $next_input_file = 0;
foreach my $infile (@ARGV)
{
	my $in_struct;
	my $subline;
	my $is_node_struct;
	my $supertype;
	my $supertype_field;

	my $node_attrs = '';
	my $node_attrs_lineno;
	my @my_fields;
	my %my_field_types;
	my %my_field_attrs;

	# open file with name from command line, which may have a path prefix
	open my $ifh, '<', $infile or die "could not open \"$infile\": $!";

	# now shorten filename for use below
	$infile =~ s!.*src/include/!!;

	# check it against next member of @all_input_files
	die "wrong input file ordering, expected @all_input_files\n"
	  if ($infile ne $all_input_files[$next_input_file]);
	$next_input_file++;

	my $raw_file_content = do { local $/; <$ifh> };

	# strip C comments, preserving newlines so we can count lines correctly
	my $file_content = '';
	while ($raw_file_content =~ m{^(.*?)(/\*.*?\*/)(.*)$}s)
	{
		$file_content .= $1;
		my $comment = $2;
		$raw_file_content = $3;
		$comment =~ tr/\n//cd;
		$file_content .= $comment;
	}
	$file_content .= $raw_file_content;

	my $lineno = 0;
	my $prevline = '';
	foreach my $line (split /\n/, $file_content)
	{
		# per-physical-line processing
		$lineno++;
		chomp $line;
		$line =~ s/\s*$//;
		next if $line eq '';
		next if $line =~ /^#(define|ifdef|endif)/;

		# within a struct, don't process until we have whole logical line
		if ($in_struct && $subline > 0)
		{
			if ($line =~ /;$/)
			{
				# found the end, re-attach any previous line(s)
				$line = $prevline . $line;
				$prevline = '';
			}
			elsif ($prevline eq ''
				&& $line =~ /^\s*pg_node_attr\(([\w(), ]*)\)$/)
			{
				# special case: node-attributes line doesn't end with semi
			}
			else
			{
				# set it aside for a moment
				$prevline .= $line . ' ';
				next;
			}
		}

		# we are analyzing a struct definition
		if ($in_struct)
		{
			$subline++;

			# first line should have opening brace
			if ($subline == 1)
			{
				$is_node_struct = 0;
				$supertype = undef;
				next if $line eq '{';
				die "$infile:$lineno: expected opening brace\n";
			}
			# second line could be node attributes
			elsif ($subline == 2
				&& $line =~ /^\s*pg_node_attr\(([\w(), ]*)\)$/)
			{
				$node_attrs = $1;
				$node_attrs_lineno = $lineno;
				# hack: don't count the line
				$subline--;
				next;
			}
			# next line should have node tag or supertype
			elsif ($subline == 2)
			{
				if ($line =~ /^\s*NodeTag\s+type;/)
				{
					$is_node_struct = 1;
					next;
				}
				elsif ($line =~ /\s*(\w+)\s+(\w+);/ and elem $1, @node_types)
				{
					$is_node_struct = 1;
					$supertype = $1;
					$supertype_field = $2;
					next;
				}
			}

			# end of struct
			if ($line =~ /^\}\s*(?:\Q$in_struct\E\s*)?;$/)
			{
				if ($is_node_struct)
				{
					# This is the end of a node struct definition.
					# Save everything we have collected.

					foreach my $attr (split /,\s*/, $node_attrs)
					{
						if ($attr eq 'abstract')
						{
							push @abstract_types, $in_struct;
						}
						elsif ($attr eq 'custom_copy_equal')
						{
							push @custom_copy_equal, $in_struct;
						}
						elsif ($attr eq 'custom_read_write')
						{
							push @custom_read_write, $in_struct;
						}
						elsif ($attr eq 'custom_query_jumble')
						{
							push @custom_query_jumble, $in_struct;
						}
						elsif ($attr eq 'no_copy')
						{
							push @no_copy, $in_struct;
						}
						elsif ($attr eq 'no_equal')
						{
							push @no_equal, $in_struct;
						}
						elsif ($attr eq 'no_copy_equal')
						{
							push @no_copy, $in_struct;
							push @no_equal, $in_struct;
						}
						elsif ($attr eq 'no_query_jumble')
						{
							push @no_query_jumble, $in_struct;
						}
						elsif ($attr eq 'no_read')
						{
							push @no_read, $in_struct;
						}
						elsif ($attr eq 'nodetag_only')
						{
							push @nodetag_only, $in_struct;
						}
						elsif ($attr eq 'special_read_write')
						{
							push @special_read_write, $in_struct;
						}
						elsif ($attr =~ /^nodetag_number\((\d+)\)$/)
						{
							$manual_nodetag_number{$in_struct} = $1;
						}
						else
						{
							die
							  "$infile:$node_attrs_lineno: unrecognized attribute \"$attr\"\n";
						}
					}

					# node name
					push @node_types, $in_struct;

					# field names, types, attributes
					my @f = @my_fields;
					my %ft = %my_field_types;
					my %fa = %my_field_attrs;

					# If there is a supertype, add those fields, too.
					if ($supertype)
					{
						my @superfields;
						foreach
						  my $sf (@{ $node_type_info{$supertype}->{fields} })
						{
							my $fn = "${supertype_field}.$sf";
							push @superfields, $fn;
							$ft{$fn} =
							  $node_type_info{$supertype}->{field_types}{$sf};
							if ($node_type_info{$supertype}
								->{field_attrs}{$sf})
							{
								# Copy any attributes, adjusting array_size field references
								my @newa = @{ $node_type_info{$supertype}
									  ->{field_attrs}{$sf} };
								foreach my $a (@newa)
								{
									$a =~
									  s/array_size\((\w+)\)/array_size(${supertype_field}.$1)/;
								}
								$fa{$fn} = \@newa;
							}
						}
						unshift @f, @superfields;
					}
					# save in global info structure
					$node_type_info{$in_struct}->{fields} = \@f;
					$node_type_info{$in_struct}->{field_types} = \%ft;
					$node_type_info{$in_struct}->{field_attrs} = \%fa;

					# Propagate nodetag_only marking from files to nodes
					push @nodetag_only, $in_struct
					  if (elem $infile, @nodetag_only_files);

					# Propagate some node attributes from supertypes
					if ($supertype)
					{
						push @no_copy, $in_struct
						  if elem $supertype, @no_copy;
						push @no_equal, $in_struct
						  if elem $supertype, @no_equal;
						push @no_read, $in_struct
						  if elem $supertype, @no_read;
						push @no_query_jumble, $in_struct
						  if elem $supertype, @no_query_jumble;
						push @custom_read_write, $in_struct
						  if elem $supertype, @custom_read_write;
					}
				}

				# start new cycle
				$in_struct = undef;
				$node_attrs = '';
				@my_fields = ();
				%my_field_types = ();
				%my_field_attrs = ();
			}
			# normal struct field
			elsif ($line =~
				/^\s*(.+)\s*\b(\w+)(\[[\w\s+]+\])?\s*(?:pg_node_attr\(([\w(), ]*)\))?;/
			  )
			{
				if ($is_node_struct)
				{
					my $type = $1;
					my $name = $2;
					my $array_size = $3;
					my $attrs = $4;

					# strip "const"
					$type =~ s/^const\s*//;
					# strip trailing space
					$type =~ s/\s*$//;
					# strip space between type and "*" (pointer) */
					$type =~ s/\s+\*$/*/;
					# strip space between type and "**" (array of pointers) */
					$type =~ s/\s+\*\*$/**/;

					die
					  "$infile:$lineno: cannot parse data type in \"$line\"\n"
					  if $type eq '';

					my @attrs;
					if ($attrs)
					{
						@attrs = split /,\s*/, $attrs;
						foreach my $attr (@attrs)
						{
							if (   $attr !~ /^array_size\(\w+\)$/
								&& $attr !~ /^copy_as\(\w+\)$/
								&& $attr !~ /^read_as\(\w+\)$/
								&& !elem $attr,
								qw(copy_as_scalar
								equal_as_scalar
								equal_ignore
								equal_ignore_if_zero
								query_jumble_ignore
								query_jumble_location
								read_write_ignore
								write_only_relids
								write_only_nondefault_pathtarget
								write_only_req_outer))
							{
								die
								  "$infile:$lineno: unrecognized attribute \"$attr\"\n";
							}
						}
					}

					$type = $type . $array_size if $array_size;
					push @my_fields, $name;
					$my_field_types{$name} = $type;
					$my_field_attrs{$name} = \@attrs;
				}
			}
			# function pointer field
			elsif ($line =~
				/^\s*([\w\s*]+)\s*\(\*(\w+)\)\s*\((.*)\)\s*(?:pg_node_attr\(([\w(), ]*)\))?;/
			  )
			{
				if ($is_node_struct)
				{
					my $type = $1;
					my $name = $2;
					my $args = $3;
					my $attrs = $4;

					my @attrs;
					if ($attrs)
					{
						@attrs = split /,\s*/, $attrs;
						foreach my $attr (@attrs)
						{
							if (   $attr !~ /^copy_as\(\w+\)$/
								&& $attr !~ /^read_as\(\w+\)$/
								&& !elem $attr,
								qw(equal_ignore read_write_ignore))
							{
								die
								  "$infile:$lineno: unrecognized attribute \"$attr\"\n";
							}
						}
					}

					push @my_fields, $name;
					$my_field_types{$name} = 'function pointer';
					$my_field_attrs{$name} = \@attrs;
				}
			}
			else
			{
				# We're not too picky about what's outside structs,
				# but we'd better understand everything inside.
				die "$infile:$lineno: could not parse \"$line\"\n";
			}
		}
		# not in a struct
		else
		{
			# start of a struct?
			if ($line =~ /^(?:typedef )?struct (\w+)$/ && $1 ne 'Node')
			{
				$in_struct = $1;
				$subline = 0;
			}
			# one node type typedef'ed directly from another
			elsif ($line =~ /^typedef (\w+) (\w+);$/ and elem $1, @node_types)
			{
				my $alias_of = $1;
				my $n = $2;

				# copy everything over
				push @node_types, $n;
				my @f = @{ $node_type_info{$alias_of}->{fields} };
				my %ft = %{ $node_type_info{$alias_of}->{field_types} };
				my %fa = %{ $node_type_info{$alias_of}->{field_attrs} };
				$node_type_info{$n}->{fields} = \@f;
				$node_type_info{$n}->{field_types} = \%ft;
				$node_type_info{$n}->{field_attrs} = \%fa;
			}
			# collect enum names
			elsif ($line =~ /^typedef enum (\w+)(\s*\/\*.*)?$/)
			{
				push @enum_types, $1;
			}
		}
	}

	if ($in_struct)
	{
		die "runaway \"$in_struct\" in file \"$infile\"\n";
	}

	close $ifh;
}    # for each file


## write output

my $tmpext = ".tmp$$";

# opening boilerplate for output files
my $header_comment =
  '/*-------------------------------------------------------------------------
 *
 * %s
 *    Generated node infrastructure code
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/nodes/gen_node_support.pl
 *
 *-------------------------------------------------------------------------
 */
';


# nodetags.h

push @output_files, 'nodetags.h';
open my $nt, '>', "$output_path/nodetags.h$tmpext"
  or die "$output_path/nodetags.h$tmpext: $!";

printf $nt $header_comment, 'nodetags.h';

my $tagno = 0;
my $last_tag = undef;
foreach my $n (@node_types, @extra_tags)
{
	next if elem $n, @abstract_types;
	if (defined $manual_nodetag_number{$n})
	{
		# do not change $tagno or $last_tag
		print $nt "\tT_${n} = $manual_nodetag_number{$n},\n";
	}
	else
	{
		$tagno++;
		$last_tag = $n;
		print $nt "\tT_${n} = $tagno,\n";
	}
}

# verify that last auto-assigned nodetag stays stable
die "ABI stability break: last nodetag is $last_tag not $last_nodetag\n"
  if (defined $last_nodetag && $last_nodetag ne $last_tag);
die
  "ABI stability break: last nodetag number is $tagno not $last_nodetag_no\n"
  if (defined $last_nodetag_no && $last_nodetag_no != $tagno);

close $nt;


# make #include lines necessary to pull in all the struct definitions
my $node_includes = '';
foreach my $infile (sort @ARGV)
{
	$infile =~ s!.*src/include/!!;
	$node_includes .= qq{#include "$infile"\n};
}


# copyfuncs.c, equalfuncs.c

push @output_files, 'copyfuncs.funcs.c';
open my $cff, '>', "$output_path/copyfuncs.funcs.c$tmpext" or die $!;
push @output_files, 'equalfuncs.funcs.c';
open my $eff, '>', "$output_path/equalfuncs.funcs.c$tmpext" or die $!;
push @output_files, 'copyfuncs.switch.c';
open my $cfs, '>', "$output_path/copyfuncs.switch.c$tmpext" or die $!;
push @output_files, 'equalfuncs.switch.c';
open my $efs, '>', "$output_path/equalfuncs.switch.c$tmpext" or die $!;

printf $cff $header_comment, 'copyfuncs.funcs.c';
printf $eff $header_comment, 'equalfuncs.funcs.c';
printf $cfs $header_comment, 'copyfuncs.switch.c';
printf $efs $header_comment, 'equalfuncs.switch.c';

# add required #include lines to each file set
print $cff $node_includes;
print $eff $node_includes;

foreach my $n (@node_types)
{
	next if elem $n, @abstract_types;
	next if elem $n, @nodetag_only;
	my $struct_no_copy = (elem $n, @no_copy);
	my $struct_no_equal = (elem $n, @no_equal);
	next if $struct_no_copy && $struct_no_equal;

	print $cfs "\t\tcase T_${n}:\n"
	  . "\t\t\tretval = _copy${n}(from);\n"
	  . "\t\t\tbreak;\n"
	  unless $struct_no_copy;

	print $efs "\t\tcase T_${n}:\n"
	  . "\t\t\tretval = _equal${n}(a, b);\n"
	  . "\t\t\tbreak;\n"
	  unless $struct_no_equal;

	next if elem $n, @custom_copy_equal;

	print $cff "
static $n *
_copy${n}(const $n *from)
{
\t${n} *newnode = makeNode($n);

" unless $struct_no_copy;

	print $eff "
static bool
_equal${n}(const $n *a, const $n *b)
{
" unless $struct_no_equal;

	# track already-processed fields to support field order checks
	my %previous_fields;

	# print instructions for each field
	foreach my $f (@{ $node_type_info{$n}->{fields} })
	{
		my $t = $node_type_info{$n}->{field_types}{$f};
		my @a = @{ $node_type_info{$n}->{field_attrs}{$f} };
		my $copy_ignore = $struct_no_copy;
		my $equal_ignore = $struct_no_equal;

		# extract per-field attributes
		my $array_size_field;
		my $copy_as_field;
		my $copy_as_scalar = 0;
		my $equal_as_scalar = 0;
		foreach my $a (@a)
		{
			if ($a =~ /^array_size\(([\w.]+)\)$/)
			{
				$array_size_field = $1;
				# insist that we copy or compare the array size first!
				die
				  "array size field $array_size_field for field $n.$f must precede $f\n"
				  if (!$previous_fields{$array_size_field});
			}
			elsif ($a =~ /^copy_as\(([\w.]+)\)$/)
			{
				$copy_as_field = $1;
			}
			elsif ($a eq 'copy_as_scalar')
			{
				$copy_as_scalar = 1;
			}
			elsif ($a eq 'equal_as_scalar')
			{
				$equal_as_scalar = 1;
			}
			elsif ($a eq 'equal_ignore')
			{
				$equal_ignore = 1;
			}
		}

		# override type-specific copy method if requested
		if (defined $copy_as_field)
		{
			print $cff "\tnewnode->$f = $copy_as_field;\n"
			  unless $copy_ignore;
			$copy_ignore = 1;
		}
		elsif ($copy_as_scalar)
		{
			print $cff "\tCOPY_SCALAR_FIELD($f);\n"
			  unless $copy_ignore;
			$copy_ignore = 1;
		}

		# override type-specific equal method if requested
		if ($equal_as_scalar)
		{
			print $eff "\tCOMPARE_SCALAR_FIELD($f);\n"
			  unless $equal_ignore;
			$equal_ignore = 1;
		}

		# select instructions by field type
		if ($t eq 'char*')
		{
			print $cff "\tCOPY_STRING_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_STRING_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			print $cff "\tCOPY_BITMAPSET_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_BITMAPSET_FIELD($f);\n"
			  unless $equal_ignore;
		}
		elsif ($t eq 'int' && $f =~ 'location$')
		{
			print $cff "\tCOPY_LOCATION_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_LOCATION_FIELD($f);\n" unless $equal_ignore;
		}
		elsif (elem $t, @scalar_types or elem $t, @enum_types)
		{
			print $cff "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			if (elem 'equal_ignore_if_zero', @a)
			{
				print $eff
				  "\tif (a->$f != b->$f && a->$f != 0 && b->$f != 0)\n\t\treturn false;\n";
			}
			else
			{
				# All CoercionForm fields are treated as equal_ignore
				print $eff "\tCOMPARE_SCALAR_FIELD($f);\n"
				  unless $equal_ignore || $t eq 'CoercionForm';
			}
		}
		# arrays of scalar types
		elsif ($t =~ /^(\w+)\*$/ and elem $1, @scalar_types)
		{
			my $tt = $1;
			if (!defined $array_size_field)
			{
				warn "copyfuncs/equalfuncs: skipping field \"$n.$f\":"
				  . " no array_size attribute for pointer type \"$t\"\n";
				next;
			}
			if ($node_type_info{$n}->{field_types}{$array_size_field} eq
				'List*')
			{
				print $cff
				  "\tCOPY_POINTER_FIELD($f, list_length(from->$array_size_field) * sizeof($tt));\n"
				  unless $copy_ignore;
				print $eff
				  "\tCOMPARE_POINTER_FIELD($f, list_length(a->$array_size_field) * sizeof($tt));\n"
				  unless $equal_ignore;
			}
			else
			{
				print $cff
				  "\tCOPY_POINTER_FIELD($f, from->$array_size_field * sizeof($tt));\n"
				  unless $copy_ignore;
				print $eff
				  "\tCOMPARE_POINTER_FIELD($f, a->$array_size_field * sizeof($tt));\n"
				  unless $equal_ignore;
			}
		}
		elsif ($t eq 'function pointer')
		{
			# we can copy and compare as a scalar
			print $cff "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_SCALAR_FIELD($f);\n" unless $equal_ignore;
		}
		# node type
		elsif (($t =~ /^(\w+)\*$/ or $t =~ /^struct\s+(\w+)\*$/)
			and elem $1, @node_types)
		{
			# If the referenced type has no copy/equal support, skip the
			# field silently (it will be omitted from the generated function).
			# This matches the behaviour of hand-written functions that simply
			# leave such fields absent.
			if (   (elem $1, @no_copy or elem $1, @nodetag_only)
				&& $1 ne 'List'
				&& !$copy_ignore)
			{
				warn "copyfuncs: skipping field \"$n.$f\":"
				  . " type \"$1\" has no copy support\n";
				$copy_ignore = 1;
			}
			if (   (elem $1, @no_equal or elem $1, @nodetag_only)
				&& $1 ne 'List'
				&& !$equal_ignore)
			{
				warn "equalfuncs: skipping field \"$n.$f\":"
				  . " type \"$1\" has no equal support\n";
				$equal_ignore = 1;
			}

			print $cff "\tCOPY_NODE_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_NODE_FIELD($f);\n" unless $equal_ignore;
		}
		# array (inline)
		elsif ($t =~ /^\w+\[\w+\]$/)
		{
			print $cff "\tCOPY_ARRAY_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_ARRAY_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'struct CustomPathMethods*'
			|| $t eq 'struct CustomScanMethods*')
		{
			# Fields of these types are required to be a pointer to a
			# static table of callback functions.  So we don't copy
			# the table itself, just reference the original one.
			print $cff "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_SCALAR_FIELD($f);\n" unless $equal_ignore;
		}
		else
		{
			warn "copyfuncs/equalfuncs: skipping field \"$n.$f\":"
			  . " unhandled type \"$t\"\n";
		}

		$previous_fields{$f} = 1;
	}

	print $cff "
\treturn newnode;
}
" unless $struct_no_copy;
	print $eff "
\treturn true;
}
" unless $struct_no_equal;
}

close $cff;
close $eff;
close $cfs;
close $efs;


# readfuncs.funcs.c and readfuncs.switch.c

push @output_files, 'readfuncs.funcs.c';
open my $rff, '>', "$output_path/readfuncs.funcs.c$tmpext" or die $!;
push @output_files, 'readfuncs.switch.c';
open my $rfs, '>', "$output_path/readfuncs.switch.c$tmpext" or die $!;

printf $rff $header_comment, 'readfuncs.funcs.c';
printf $rfs $header_comment, 'readfuncs.switch.c';
print $rff $node_includes;

# Mapping from scalar C types to READ_* macros.
# Types in @scalar_types not listed here fall back to READ_INT_FIELD.
my %scalar_read_macro = (
	'bool'             => 'READ_BOOL_FIELD',
	'char'             => 'READ_CHAR_FIELD',
	'double'           => 'READ_FLOAT_FIELD',
	'Cardinality'      => 'READ_FLOAT_FIELD',
	'Cost'             => 'READ_FLOAT_FIELD',
	'Selectivity'      => 'READ_FLOAT_FIELD',
	'int64'            => 'READ_LONG_FIELD',
	'long'             => 'READ_LONG_FIELD',
	'Oid'              => 'READ_OID_FIELD',
	'RelFileNumber'    => 'READ_OID_FIELD',
	'uint64'           => 'READ_UINT64_FIELD',
	'XLogRecPtr'       => 'READ_UINT64_FIELD',
	'uint32'           => 'READ_UINT_FIELD',
	'bits32'           => 'READ_UINT_FIELD',
	'AclMode'          => 'READ_UINT_FIELD',
	'Index'            => 'READ_UINT_FIELD',
	'SubTransactionId' => 'READ_UINT_FIELD',
	'TimeLineID'       => 'READ_UINT_FIELD',
	'uint8'            => 'READ_UINT_FIELD',
	'uint16'           => 'READ_UINT_FIELD',
	'StrategyNumber'   => 'READ_UINT_FIELD',
	'Size'             => 'READ_UINT_FIELD',
);

my $first_read_switch = 1;

foreach my $n (@node_types)
{
	next if elem $n, @abstract_types;
	next if elem $n, @nodetag_only;
	next if elem $n, @no_read;
	next if elem $n, @special_read_write;

	# Generate switch entry for all remaining nodes (auto and custom).
	my $uc_n = uc($n);
	if ($first_read_switch)
	{
		print $rfs "\tif (MATCHX(\"$uc_n\"))\n\t\treturn_value = _read${n}();\n";
		$first_read_switch = 0;
	}
	else
	{
		print $rfs "\telse if (MATCHX(\"$uc_n\"))\n\t\treturn_value = _read${n}();\n";
	}

	# custom_read_write nodes get a switch entry but no generated function body.
	next if elem $n, @custom_read_write;

	# Buffer the function text so we can discard it if any field is
	# unhandleable (rather than aborting the whole generation run).
	# Use READ_LOCALS_PLACEHOLDER as a sentinel; replace with either
	# READ_LOCALS or READ_LOCALS_NO_FIELDS depending on whether any
	# field reads are generated (to avoid unused-variable warnings for
	# empty structs that have no fields to read).
	my $func_buf = "\nstatic $n *\n_read${n}(void)\n{\n\tREAD_LOCALS_PLACEHOLDER($n);\n";
	my $skip_node = 0;
	my $has_field_reads = 0;

	foreach my $f (@{ $node_type_info{$n}->{fields} })
	{
		my $t  = $node_type_info{$n}->{field_types}{$f};
		my @a  = @{ $node_type_info{$n}->{field_attrs}{$f} // [] };

		# Determine per-field read behavior from attributes.
		my $read_ignore = 0;
		my $read_as_val;
		my $array_size_field;

		foreach my $a (@a)
		{
			if (   $a eq 'read_write_ignore'
				|| $a eq 'write_only_relids'
				|| $a eq 'write_only_nondefault_pathtarget'
				|| $a eq 'write_only_req_outer')
			{
				$read_ignore = 1;
			}
			elsif ($a =~ /^read_as\((\w+)\)$/)
			{
				$read_as_val = $1;
			}
			elsif ($a =~ /^array_size\(([\w.]+)\)$/)
			{
				$array_size_field = $1;
			}
		}

		# If read_as is set but read_write_ignore is not, set the field value.
		if (defined $read_as_val && !$read_ignore)
		{
			$func_buf .= "\tlocal_node->$f = $read_as_val;\n";
			next;
		}
		next if $read_ignore;

		if (defined $array_size_field)
		{
			# Array field — use the appropriate READ_*_ARRAY macro.
			if ($t =~ /^(\w+)\*$/)
			{
				my $elem_t = $1;

				# Compute array length expression from the size field.
				my $size_field_t =
				  $node_type_info{$n}->{field_types}{$array_size_field};
				my $size_expr =
				  (defined $size_field_t && $size_field_t eq 'List*')
				  ? "list_length(local_node->$array_size_field)"
				  : "local_node->$array_size_field";

				if ($elem_t eq 'AttrNumber' || $elem_t eq 'int16')
				{
					$func_buf .= "\tREAD_ATTRNUMBER_ARRAY($f, $size_expr);\n";
				}
				elsif ($elem_t eq 'Oid')
				{
					$func_buf .= "\tREAD_OID_ARRAY($f, $size_expr);\n";
				}
				elsif ($elem_t eq 'int')
				{
					$func_buf .= "\tREAD_INT_ARRAY($f, $size_expr);\n";
				}
				elsif ($elem_t eq 'bool')
				{
					$func_buf .= "\tREAD_BOOL_ARRAY($f, $size_expr);\n";
				}
				else
				{
					warn "readfuncs: cannot handle array element type"
					  . " \"$elem_t\" in struct \"$n\" field \"$f\" --"
					  . " marking as custom_read_write\n";
					$skip_node = 1;
					last;
				}
			}
			else
			{
				warn "readfuncs: array_size on non-pointer field"
				  . " \"$f\" in struct \"$n\" --"
				  . " marking as custom_read_write\n";
				$skip_node = 1;
				last;
			}
		}
		elsif ($t eq 'char*')
		{
			$func_buf .= "\tREAD_STRING_FIELD($f);\n";
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			$func_buf .= "\tREAD_BITMAPSET_FIELD($f);\n";
		}
		elsif ($t eq 'int' && $f =~ /location$/)
		{
			$func_buf .= "\tREAD_LOCATION_FIELD($f);\n";
		}
		elsif (exists $scalar_read_macro{$t})
		{
			$func_buf .= "\t$scalar_read_macro{$t}($f);\n";
		}
		elsif (elem $t, @scalar_types)
		{
			$func_buf .= "\tREAD_INT_FIELD($f);\n";
		}
		elsif (elem $t, @enum_types)
		{
			$func_buf .= "\tREAD_ENUM_FIELD($f, $t);\n";
		}
		elsif ($t eq 'function pointer')
		{
			warn "readfuncs: function pointer in struct \"$n\" field"
			  . " \"$f\" -- marking as custom_read_write\n";
			$skip_node = 1;
			last;
		}
		elsif (   ($t =~ /^(\w+)\*$/ or $t =~ /^struct\s+(\w+)\*$/)
			&& elem $1, @node_types)
		{
			$func_buf .= "\tREAD_NODE_FIELD($f);\n";
		}
		elsif ($t =~ /^\w+\[\w+\]$/)
		{
			warn "readfuncs: inline array type \"$t\" in struct \"$n\""
			  . " field \"$f\" -- marking as custom_read_write\n";
			$skip_node = 1;
			last;
		}
		else
		{
			warn "readfuncs: unhandled type \"$t\" in struct \"$n\""
			  . " field \"$f\" -- marking as custom_read_write\n";
			$skip_node = 1;
			last;
		}
	}

	next if $skip_node;

	$func_buf .= "\n\tREAD_DONE();\n}\n";

	# Replace the placeholder: use READ_LOCALS_NO_FIELDS if no field reads
	# were emitted (to avoid -Werror=unused-variable for token/length vars).
	if ($func_buf =~ /\tREAD_LOCALS_PLACEHOLDER\($n\);\n\n\tREAD_DONE\(\);\n\}/)
	{
		$func_buf =~ s/\tREAD_LOCALS_PLACEHOLDER\($n\);/\tREAD_LOCALS_NO_FIELDS($n);/;
	}
	else
	{
		$func_buf =~ s/\tREAD_LOCALS_PLACEHOLDER\($n\);/\tREAD_LOCALS($n);/;
	}
	print $rff $func_buf;
}

close $rff;
close $rfs;


# outfuncs.funcs.c and outfuncs.switch.c

push @output_files, 'outfuncs.funcs.c';
open my $off, '>', "$output_path/outfuncs.funcs.c$tmpext" or die $!;
push @output_files, 'outfuncs.switch.c';
open my $ofs, '>', "$output_path/outfuncs.switch.c$tmpext" or die $!;

printf $off $header_comment, 'outfuncs.funcs.c';
printf $ofs $header_comment, 'outfuncs.switch.c';
print $off $node_includes;

# Mapping from scalar C types to WRITE_* macros.
# Types in @scalar_types not listed here fall back to WRITE_INT_FIELD.
my %scalar_write_macro = (
	'bool'             => 'WRITE_BOOL_FIELD',
	'char'             => 'WRITE_CHAR_FIELD',
	'double'           => 'WRITE_FLOAT_FIELD',
	'Cardinality'      => 'WRITE_FLOAT_FIELD',
	'Cost'             => 'WRITE_FLOAT_FIELD',
	'Selectivity'      => 'WRITE_FLOAT_FIELD',
	'int64'            => 'WRITE_LONG_FIELD',
	'long'             => 'WRITE_LONG_FIELD',
	'Oid'              => 'WRITE_OID_FIELD',
	'RelFileNumber'    => 'WRITE_OID_FIELD',
	'uint64'           => 'WRITE_UINT64_FIELD',
	'XLogRecPtr'       => 'WRITE_UINT64_FIELD',
	'uint32'           => 'WRITE_UINT_FIELD',
	'bits32'           => 'WRITE_UINT_FIELD',
	'AclMode'          => 'WRITE_UINT_FIELD',
	'Index'            => 'WRITE_UINT_FIELD',
	'SubTransactionId' => 'WRITE_UINT_FIELD',
	'TimeLineID'       => 'WRITE_UINT_FIELD',
	'uint8'            => 'WRITE_UINT_FIELD',
	'uint16'           => 'WRITE_UINT_FIELD',
	'StrategyNumber'   => 'WRITE_UINT_FIELD',
	'Size'             => 'WRITE_UINT_FIELD',
);

foreach my $n (@node_types)
{
	next if elem $n, @abstract_types;
	next if elem $n, @nodetag_only;
	next if elem $n, @special_read_write;

	# Generate switch entry for all remaining nodes (auto and custom).
	my $uc_n = uc($n);
	print $ofs "\tcase T_${n}:\n\t\t_out${n}(str, obj);\n\t\tbreak;\n";

	# custom_read_write nodes get a switch entry but no generated function body.
	next if elem $n, @custom_read_write;

	# Buffer the function text so we can discard it if any field is
	# unhandleable (rather than aborting the whole generation run).
	my $func_buf =
	  "\nstatic void\n_out${n}(StringInfo str, const ${n} *node)\n"
	  . "{\n\tWRITE_NODE_TYPE(\"${uc_n}\");\n\n";
	my $skip_node = 0;

	foreach my $f (@{ $node_type_info{$n}->{fields} })
	{
		my $t  = $node_type_info{$n}->{field_types}{$f};
		my @a  = @{ $node_type_info{$n}->{field_attrs}{$f} // [] };

		# Determine per-field write behavior from attributes.
		my $write_ignore = 0;
		my $array_size_field;

		foreach my $a (@a)
		{
			if (   $a eq 'read_write_ignore'
				|| $a =~ /^read_as\(/)
			{
				$write_ignore = 1;
			}
			elsif ($a eq 'write_only_relids'
				|| $a eq 'write_only_nondefault_pathtarget'
				|| $a eq 'write_only_req_outer')
			{
				# These require specialized write logic; cannot be auto-generated.
				warn "outfuncs: write_only attribute in struct \"$n\" field"
				  . " \"$f\" -- marking as custom_read_write\n";
				$skip_node = 1;
				last;
			}
			elsif ($a =~ /^array_size\(([\w.]+)\)$/)
			{
				$array_size_field = $1;
			}
		}
		last if $skip_node;
		next if $write_ignore;

		if (defined $array_size_field)
		{
			# Array field -- use the appropriate WRITE_*_ARRAY macro.
			my $size_expr = "node->$array_size_field";

			if ($t =~ /^(?:struct\s+)?(\w+)\*\*$/ && elem $1, @node_types)
			{
				# Array of node pointers.
				$func_buf .= "\tWRITE_NODE_ARRAY($f, $size_expr);\n";
			}
			elsif ($t =~ /^(\w+)\*$/)
			{
				my $elem_t = $1;

				if ($elem_t eq 'AttrNumber' || $elem_t eq 'int16')
				{
					$func_buf .= "\tWRITE_ATTRNUMBER_ARRAY($f, $size_expr);\n";
				}
				elsif ($elem_t eq 'Oid')
				{
					$func_buf .= "\tWRITE_OID_ARRAY($f, $size_expr);\n";
				}
				elsif ($elem_t eq 'int')
				{
					$func_buf .= "\tWRITE_INT_ARRAY($f, $size_expr);\n";
				}
				elsif ($elem_t eq 'bool')
				{
					$func_buf .= "\tWRITE_BOOL_ARRAY($f, $size_expr);\n";
				}
				else
				{
					warn "outfuncs: cannot handle array element type"
					  . " \"$elem_t\" in struct \"$n\" field \"$f\" --"
					  . " marking as custom_read_write\n";
					$skip_node = 1;
					last;
				}
			}
			else
			{
				warn "outfuncs: array_size on unrecognized field type"
				  . " \"$t\" in struct \"$n\" field \"$f\" --"
				  . " marking as custom_read_write\n";
				$skip_node = 1;
				last;
			}
		}
		elsif ($t eq 'char*')
		{
			$func_buf .= "\tWRITE_STRING_FIELD($f);\n";
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			$func_buf .= "\tWRITE_BITMAPSET_FIELD($f);\n";
		}
		elsif ($t eq 'int' && $f =~ /location$/)
		{
			$func_buf .= "\tWRITE_LOCATION_FIELD($f);\n";
		}
		elsif (exists $scalar_write_macro{$t})
		{
			$func_buf .= "\t$scalar_write_macro{$t}($f);\n";
		}
		elsif (elem $t, @scalar_types)
		{
			$func_buf .= "\tWRITE_INT_FIELD($f);\n";
		}
		elsif (elem $t, @enum_types)
		{
			$func_buf .= "\tWRITE_ENUM_FIELD($f, $t);\n";
		}
		elsif ($t eq 'function pointer')
		{
			warn "outfuncs: function pointer in struct \"$n\" field"
			  . " \"$f\" -- marking as custom_read_write\n";
			$skip_node = 1;
			last;
		}
		elsif (   ($t =~ /^(\w+)\*$/ or $t =~ /^struct\s+(\w+)\*$/)
			&& elem $1, @node_types)
		{
			$func_buf .= "\tWRITE_NODE_FIELD($f);\n";
		}
		elsif ($t =~ /^\w+\[\w+\]$/)
		{
			warn "outfuncs: inline array type \"$t\" in struct \"$n\""
			  . " field \"$f\" -- marking as custom_read_write\n";
			$skip_node = 1;
			last;
		}
		else
		{
			warn "outfuncs: unhandled type \"$t\" in struct \"$n\""
			  . " field \"$f\" -- marking as custom_read_write\n";
			$skip_node = 1;
			last;
		}
	}

	next if $skip_node;

	$func_buf .= "}\n";
	print $off $func_buf;
}

close $off;
close $ofs;


# now rename the temporary files to their final names
foreach my $file (@output_files)
{
	Catalog::RenameTempFile("$output_path/$file", $tmpext);
}


# Automatically clean up any temp files if the script fails.
END
{
	# take care not to change the script's exit value
	my $exit_code = $?;

	if ($exit_code != 0)
	{
		foreach my $file (@output_files)
		{
			unlink("$output_path/$file$tmpext");
		}
	}

	$? = $exit_code;
}
