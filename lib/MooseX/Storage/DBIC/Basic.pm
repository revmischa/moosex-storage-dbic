package MooseX::Storage::DBIC::Basic;

# overrides for MooseX::Storage::Basic to simplify calls to pack/unpack
use Moose::Role;
use namespace::autoclean;
with 'MooseX::Storage::Basic';
use MooseX::Storage::DBIC::Engine::Traits::Default;
use Data::Dump qw/ddx pp/;
use Scalar::Util qw/reftype refaddr blessed/;

requires 'schema';

sub add_mxstorage_dbic_engine_traits {
    my ($self, $opts) = @_;

    my $traits = $opts->{engine_traits} || [];
    my $engine = '+MooseX::Storage::DBIC::Engine::Traits::Default';
    push @$traits, $engine unless grep { $_ eq $engine } @$traits;

    $opts->{engine_traits} = $traits;
}

sub isa_dbic {
    my ($class, $obj) = @_;

    return unless $obj && (! ref($obj) || blessed($obj));
    return $obj->isa('DBIx::Class::Core');
}

sub is_storage {
    my ($class, $obj) = @_;

    return $obj && blessed($obj) && $obj->isa('Moose::Object') &&
        $obj->DOES('MooseX::Storage::DBIC');
}

*packed_storage_type = \&is_packed_storage;
sub is_packed_storage {
    my ($class, $obj) = @_;

    return $obj && ref($obj) && reftype($obj) eq 'HASH' &&
        $obj->{$MooseX::Storage::DBIC::Engine::Traits::Default::DBIC_MARKER};
}

around 'pack' => sub {
    my ($orig, $self, %opts) = @_;

    $self->add_mxstorage_dbic_engine_traits(\%opts);
    return $self->$orig(%opts);
};

around 'unpack' => sub {
    my ($orig, $self, $data, %opts) = @_;

    $self->add_mxstorage_dbic_engine_traits(\%opts);
    return $self->$orig($data, %opts);
};

around _storage_construct_instance => sub  {
    my ($orig, $class, $args, $opts) = @_;
    my %i = defined $opts->{'inject'} ? %{ $opts->{'inject'} } : ();

    # fields to directly populate
    my $fields = {};

    my $rsname = $class->packed_storage_type($args);
    #ddx($args);
    #warn "rename: $rsname";

    # recursively clean up relationship construction args
    my $clean_args; $clean_args = sub {
        my ($a, $_fields) = @_;

        return $a unless ref($a) && reftype($a) eq 'HASH';

        # is arg a DBIC row? find resultset
        my $arg_rsname = $a->{$MooseX::Storage::DBIC::Engine::Traits::Default::DBIC_MARKER};
        my $rs; $rs = $class->schema->resultset($arg_rsname) if $arg_rsname;

        # you go away too
        delete $a->{$MooseX::Storage::Engine::CLASS_MARKER};

        my $ret = {};
        my %fields = %$_fields;
        while (my ($k, $v) = each %fields) {
            #warn "rsname=$arg_rsname, k: $k, v: $v";
            if ($arg_rsname) {
                # we only want to pass columns to new_result()
                if ($rs->result_source->columns_info->{$k}) {
                    #warn "ref(a->{$k}) = " . (ref($a->{$k}));
                    if (ref($a->{$k}) && ref($a->{$k}) eq 'HASH') {
                        my $dbic_class = $class->packed_storage_type($a->{$k});
                        my $cleaned = $clean_args->($a->{$k}, $_fields->{$k});

                        if ($dbic_class) {
                            # it appears we have discovered a relationship!
                            # if we don't bless $cleaned, DBIC will try looking the rel up itself
                            #warn "blessing cleaned into $dbic_class";
                            $a->{$k} = bless($cleaned, $dbic_class);
                        } else {
                            $ret->{$k} = $cleaned;
                        }
                    } else {
                        #warn "not ref $v";
                        #delete $ret->{$k};
                    }
                } else {
                    # want to save this for setting later
                    #warn "a->{$k} = $a->{$k}";
                    my $dbic_class = $class->packed_storage_type($v);
                    if ($dbic_class) {
                        # got dbic object hashref
                        #warn "got dbic object hashref for $k";
                        $ret->{$k} = $v;
                    } else {
                        $ret->{$k} = $clean_args->($a->{$k}, $_fields->{$k});
                    }
                    delete $a->{$k};
                }
            } else {
                my $dbic_class = $class->packed_storage_type($v);

                if ($dbic_class) {
                    # got a free-floating DBIC row packed inside
                    # something that is not a DBIC row
                    #warn "got free-floating row for $k";
                    #ddx($v);
                    $ret->{$k} = $dbic_class->unpack($v);
                } else {
                    $ret->{$k} = $clean_args->($v, $_fields->{$k});
                }
            }
        }
        return $ret;
    };

    $fields = $clean_args->($args, $args);
    #ddx($fields);
    #ddx($args);

    my $result;
    my %ctor_args = ( %$args, %i );
    #ddx(\%ctor_args);
    if ($rsname) {
        # construct DBIC instance
        $result = $class->schema->resultset($rsname)->new_result(\%ctor_args);
    } else {
        # construct normal moose instance
        $result = $class->new(%ctor_args);
    }

    # directly set fields on our hashref recursively
    my $set_fields; $set_fields = sub {
        my ($hashref, $_fields) = @_;

        return unless $_fields;

        while (my ($k, $v) = each %$_fields) {
            my $has_accessor = blessed($hashref) && $hashref->can($k);
            my $set = sub {
                if ($has_accessor) {
                    # call setter
                    $hashref->$k($v);
                } else {
                    # set field directly
                    $hashref->{$k} = $v;
                }
            };

            my $dbic_class = $class->packed_storage_type($v);
            if ($dbic_class && refaddr($v) != refaddr($result)) {
                # got a dbic row as a field
                $v = $dbic_class->unpack($v);
                $set->();
                next;
            }

            if (ref($v) && reftype($v) eq 'HASH') {
                my $hv = $has_accessor ? $hashref->$k : $hashref->{$k};
                $set_fields->($hv || $v, $v);
                $hashref->{$k} = $v;
            } else {
                $set->();
            }
        }
    };
    $set_fields->($result, $fields);

    return $result;
};

1;
