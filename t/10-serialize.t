#!/usr/bin/env perl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";
use Data::Dump qw/ddx/;

BEGIN {
    # can we use fake dbic schema?
    use Test::DBIC;

    eval 'require DBD::SQLite';
    if ($@) {
        plan skip_all => 'DBD::SQLite not installed';
    } else {
        plan tests => 12;
    }
    use_ok( 'MooseX::Storage::DBIC' );
}

my $schema;

###
# resultset #1
package MXSD::RS1;
use base 'DBIx::Class';
use Moose;
use MooseX::NonMoose;
use MooseX::MarkAsMethods autoclean => 1;
extends 'DBIx::Class::Core';
use DBIx::Class::MooseColumns;
__PACKAGE__->load_components("InflateColumn::DateTime");
__PACKAGE__->table("rs1");
__PACKAGE__->add_columns(
  "id" => { data_type => "integer" },
);
__PACKAGE__->belongs_to("rs2" => "MXSD::RS2", { rs1id => "id" });

with 'MooseX::Storage::DBIC';
sub schema { $schema }
__PACKAGE__->serializable(qw/ id rs2 foo attr /);

has 'attr' => ( is => 'rw', isa => 'Str', default => 'default' );

1;

# resultset #2
package MXSD::RS2;
use base 'DBIx::Class';
use Moose;
use MooseX::NonMoose;
use MooseX::MarkAsMethods autoclean => 1;
extends 'DBIx::Class::Core';
use DBIx::Class::MooseColumns;
__PACKAGE__->table("rs2");
__PACKAGE__->add_columns(
  "id" => { data_type => "integer" },
  "rs1id" => { data_type => "integer" },
);
__PACKAGE__->has_many("rs1" => "MXSD::RS1", {});

with 'MooseX::Storage::DBIC';
sub schema { $schema }
__PACKAGE__->serializable(qw/ id rs1id baz bleh attr /);

has 'attr' => ( is => 'rw', isa => 'Str', default => 'default2' );

1;


###

package main;

run_tests();

sub run_tests {
    $schema = Test::DBIC->init_schema(
        existing_namespace => 'MXSD',
        sqlt_deploy => 1,
        'sample_data' => [
            RS1 => [
                ['id'],
                [1],
                [2],
            ],
            RS2 => [
                ['id', 'rs1id'],
                [3, 1],
                [4, 999],
            ],
        ],
    );

    my @rs1s = $schema->resultset('RS1')->all;
    my @rs2s = $schema->resultset('RS2')->all;

    # test serialization of first rs1 row, which is related to rs2
    {
        my $rs1 = shift @rs1s; # first rows
        my $rs2 = shift @rs2s;
        $rs1->attr('quux');
        $rs1->{foo} = 456;
        $rs1->rs2->{baz} = { a => [ 1, 2, 3, 4 ] };
        $rs1->rs2->{rs1id} = $rs1->id;

        my $packed = $rs1->pack;
        my $unpacked = MXSD::RS1->unpack($packed);

        # got expected results from deserialization?
        is($unpacked->attr, $rs1->attr, "Deserialized attribute");
        is($unpacked->id, $rs1->id, "Deserialized column");
        is($unpacked->rs2->rs1id, $rs2->rs1id, "Deserialized rel column");
        is($unpacked->rs2->id, $rs2->id, "Deserialized rel column");
        is($unpacked->{foo}, 456, "Deserialized field");
        is_deeply($unpacked->rs2->{baz}, $rs1->rs2->{baz}, "Deserialized rel field");
    }

    # test serializing different set of rows
    {
        my $rs1 = shift @rs1s; # second rows
        my $rs2 = shift @rs2s;

        $rs2->{bleh} = [ 1, { xyz => [ 789, 'a' ] }, 3, 4 ];
        $rs1->{foo} = 42;
        $rs1->attr('moof');
        $rs2->{baz} = $rs1;
        $rs2->{not_serialized} = 123;
        my $packed = $rs2->pack;
        my $unpacked = MXSD::RS2->unpack($packed);
        is($unpacked->{baz}->attr, $rs1->attr, "Got serialized rel attr");
        is($unpacked->{not_serialized}, undef, "Skipped non-serialized field");
        is_deeply($unpacked->{bleh}, $rs2->{bleh}, "Deserialized complex fields");
        is($unpacked->{baz}->id, $rs1->id, "Deserialized row buried in hashref");
        is($unpacked->{baz}{foo}, $rs1->{foo}, "Deserialized field in row");

        # TODO: force default attributes to be set if they aren't lazily-loaded
        #is($unpacked->attr, $rs2->attr, "Got serialized default attr");
    }
}



