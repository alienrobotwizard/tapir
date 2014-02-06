require 'pigudf'

#
# BUG: Can not run jruby udfs when Pigserver
# is created by jruby.
#
class TestUdfs

  def test tuple
    "#{tuple}-test"
  end
  
end
