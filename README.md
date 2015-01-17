# bookshelf-repl
A REPL for your Bookshelf.js projects

# Example
```JavaScript
var
  dbRepl = require('bookshelf-repl')
  knex = require('knex').initialize(getKnexConfig());

dbRepl
  .start(knex)
  .then(function(repl) {
    repl.on('exit', function onReplExit() {
      console.log('Got "exit" event from repl!');
      process.exit();
    });
  })
  .catch(console.error);

function getKnexConfig() {
  // get your knex configuration from somewhere
  return {};
}
```
