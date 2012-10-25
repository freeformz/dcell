shared_context "a DCell registry" do
  context "node registry" do
    before :each do
      subject.clear_nodes
      subject.nodes.should be_empty
    end

    it "stores node addresses" do
      address = "tcp://localhost:7777"

      subject.set_node("foobar", address)
      subject.get_node("foobar").should == address
    end

    it "updates node addresses" do
      initial_address = 'tcp://localhost:7777'
      subject.set_node("foobar", initial_address)
      subject.get_node("foobar").should == initial_address

      new_address = 'tcp://localhost:6666'
      subject.set_node("foobar", new_address)
      subject.get_node("foobar").should == new_address
    end

    it "stores the IDs of all nodes" do
      subject.set_node("foobar", "tcp://localhost:7777")
      subject.nodes.should include "foobar"
    end
  end

  context "global registry" do
    before :each do
      subject.clear_globals
      subject.global_keys.should be_empty
    end

    it "stores values" do
      value = [1,2,3]
      subject.set_global("foobar", value)
      subject.get_global("foobar").should == value
    end

    it "updates values" do
      initial_value = [1,2,3]
      subject.set_global("foobar", initial_value)
      subject.get_global("foobar").should == initial_value

      new_value = [4,5,6]
      subject.set_global("foobar", new_value)
      subject.get_global("foobar").should == new_value
    end

    it "stores the keys of all globals" do
      subject.set_global("foobar", true)
      subject.global_keys.should include "foobar"
    end
  end
end
