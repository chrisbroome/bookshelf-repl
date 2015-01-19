# bookshelf-repl
A REPL for your Bookshelf.js projects

# Example

##### Your code

```JavaScript
var
  dbRepl = require('bookshelf-repl')
  knex = require('knex').initialize(getKnexConfig()),
  options = {
    prompt: 'DB> '
  };

dbRepl
  .start(knex, options)
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

#### The repl

TODO: Document repl globals
