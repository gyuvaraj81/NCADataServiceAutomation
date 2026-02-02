function fn() {

  var python = 'python';
  var script = 'src/main/python/run_query.py';

  function buildPythonCmd(query, tablename) {

    var cmd = python + ' ' + script + ' --query ' + query;

    if (tablename) {
      cmd = cmd + ' --tablename ' + tablename;
    }

    return cmd;
  }

  return {
    python: python,
    script: script,
    buildPythonCmd: buildPythonCmd
  };
}
