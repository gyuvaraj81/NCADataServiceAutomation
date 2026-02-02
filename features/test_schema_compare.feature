Feature: Validate schema

Background:
    * def query = 'schema_compare'
    * def pythonCmd = buildPythonCmd(query)
    * print 'Inside schema_compare.feature: pythonCmd ===> ',pythonCmd

@regression
Scenario: SIT and PROD counts must match
    * call read('run-query.feature')
    #* match result.exitCode == 0
