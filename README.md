[![Build Status](https://travis-ci.org/molecuel/mongolastic.svg?branch=master)](https://travis-ci.org/molecuel/mongolastic)

[![NPM](https://nodei.co/npm-dl/mongolastic.png?months=1)](https://nodei.co/npm/mongolastic/)

[![NPM](https://nodei.co/npm/mongolastic.png?downloads=true&stars=true)](https://nodei.co/npm/mongolastic/)

[![NPM version](https://badge.fury.io/js/mongolastic@2x.png)](http://badge.fury.io/js/mongolastic)

mongolastic
===========

Mongolastic is a mongoose middleware which provides automatic index functionality for mongoose objects. Unlike other libs like elmongo this module is based on the official elasticsearch javascript api and provides full access to the api via mongolastic.connection.

It should be registered as plugin and a modelname should be provided.

```js
var mongolastic = require('mongolastic');
var mongoose = require('mongoose');

mongoose.connect('mongodb://localhost/mongolastic');

var CostumeSchema = mongoose.Schema({
  name: String,
});
CostumeSchema.plugin(mongolastic.plugin, {modelname: 'costume'});
var costume = mongoose.model('costume', CostumeSchema);
// add additional functions to the model and create the index with the mapping
mongolastic.registerModel(costume, function(err, res) {
  console.log('model registered and mapping created');
});

var CatSchema = mongoose.Schema({
  name: String,
  date: {type: Date, default: Date.now},
  // this is a sample for a additional mapping
  url: {type: String, elastic: {mapping: {type: 'string', index: 'not_analyzed'}}},
  // automatic population of references to the index + select fields to populate
  costume: {type: mongoose.Schema.ObjectId, ref: 'costume', elastic: {popfields: 'name'}}
});

// modelname is important to provide the correct index for the model
CatSchema.plugin(mongolastic.plugin, {modelname: 'cat'});
var cat = mongoose.model('cat', CatSchema);
// add additional functions to the model and create the index with the mapping
mongolastic.registerModel(cat, function(err, res) {
  console.log('model registered and mapping created');
});
```

## Additional new functions
- References can be automatically saved as denormalized (populated) version in the elasticsearch index.
- Recursive population of documents
- Render a specific mapping for document types
- Sync function
- Sync function as much more performant bulk operation
- Registration of save handlers per model. These are needed to execute advanced save handling functionality after the document has been saved to the mongodb

By the way... mongolastic is a core component of the upcoming molecuel CMS. [https://github.com/molecuel](https://github.com/molecuel)

## License (MIT)

Copyright (c) 2014 Dominic Böttger <[http://inspirationlabs.com](http://inspirationlabs.com)>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
