Feature: Validate row counts

Background:
    * def query = 'count'
    * def pythonCmd = buildPythonCmd(query)
    * print 'Inside count.feature: pythonCmd ===> ',pythonCmd
    
@regression
Scenario: SIT and PROD counts must match
    * call read('run-query.feature')
    #* match result.exitCode == 0
