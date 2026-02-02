Feature: Run run-query.py and verify outputs

  Scenario: Execute run-query.py and check output files
    # Run the Python script
    * print 'Inside run-query.feature: pythonCmd ===> ', pythonCmd
    * def result = karate.exec(pythonCmd)
	  * print 'result ===> ', result

