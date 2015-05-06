var
  dbRepl = require('../lib/db-repl'),
  knex = require('knex')({
    client: process.env.KNEX_CLIENT,
    connection: process.env.KNEX_CONNECTION
  });

dbRepl.start(knex)
  .then(function(repl) {
    repl.on('exit', onReplExit);
  })
  .catch(function(err) {
    console.error(err);
    console.error(err.message);
    console.error(err.stack);
  });

function onReplExit() {
  console.log('Got "exit" event from repl!');
  process.exit();
}
