Feature: Validate row data

Background:
    * def query = 'except_distinct_deltaload'
    * def tablename = karate.properties['tablename'] ? karate.properties['tablename'] : '*'
    * def pythonCmd = buildPythonCmd(query, tablename)
    * print 'Inside except_distinct.feature: pythonCmd ===> ',pythonCmd

@regression
Scenario: SIT and PROD counts must match
    * call read('run-query.feature')
    #* match result.exitCode == 0
