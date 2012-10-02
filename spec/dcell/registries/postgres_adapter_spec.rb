require 'spec_helper'

describe DCell::Registry::PostgresAdapter do
  subject { DCell::Registry::PostgresAdapter.new :url => 'postgres://dcell:foo@localhost/dcell_test' }
  it_behaves_like "a DCell registry"
end
