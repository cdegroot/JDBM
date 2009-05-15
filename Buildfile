# This is an Apache Buildr build file (http://incubator.apache.org/buildr/)

gem "buildr", ">=1.2.10"

define 'jdbm' do
  project.version = '1.1-SNAPSHOT'
  project.group = 'net.sf.jdbm'
  
  compile.with 'lib/cweb-extser-0.1-b2-dev.jar'
  compile.from 'src/main'
  test.compile.from 'src/tests'
  test.exclude '*CrashTest*', '*HashtableTest*', '*TestInsertPerf*', '*TestObject*',
               '*TestCachePolicy*', '*TestUtil*'
  package(:jar)
end
