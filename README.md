mongolastic
===========

Mongolastic is a mongoose middleware which provides automatic index functionality for mongoose objects. Unlike other libs like elmongo this module is based on the official elasticsearch javascript api and provides full access to the api via mongolastic.connection.

It should be registered as plugin and a modelname should be provided.

```js
var mongolastic = require('mongolastic');
var mongoose = require('mongoose');

mongoose.connect('mongodb://localhost/mongolastic');
var CatSchema = mongoose.Schema({
  name: String,
  date: {type: Date, default: Date.now}
});

// modelname is important to provide the correct index for the model
CatSchema.plugin(mongolastic.plugin, {modelname: 'cat'});
var cat = mongoose.model('cat', CatSchema);
```

##  Todo

- Sync function