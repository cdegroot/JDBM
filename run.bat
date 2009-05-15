@echo off
REM $Id: run.bat,v 1.2 2003/10/31 00:13:35 dranatunga Exp $
set JAVA=%JAVA_HOME%\bin\java
set cp=%CLASSPATH%
for %%i in (lib\*.jar) do call cp.bat %%i
set CP=%JAVA_HOME%\lib\tools.jar;build\classes;build\examples;build\tests;%CP%
rem %JAVA% -Xrunhprof:cpu=samples,depth=10 -classpath %CP% %1 %2 %3 %4 %5 %6
%JAVA% -classpath %CP% %1 %2 %3 %4 %5 %6

