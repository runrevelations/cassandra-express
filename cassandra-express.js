var express = require('express');
var coffeeShopRouter = express.Router();
var config = require('../config');
var client = config.client;
var cassandra = config.cassandra;
var debug = config.debug('coffeeShops');

// Cassandra DB queries  PRIMARY KEY = email 
var uuid = cassandra.types.Uuid.random(); // unique id generation
var queryCoffeeShops = 'SELECT * FROM businesses.coffee_shops';
var queryOneCoffeeShop = 'SELECT * FROM businesses.coffee_shops WHERE id = ? ALLOW FILTERING';
var createNewCoffeeShop = 'INSERT INTO businesses.coffee_shops (name, zip_code, phone, state, city, email, id) VALUES (:name, :zip_code, :phone, :state, :city, :email, :id)';
var updateNameWhereEmail = 'UPDATE businesses.coffee_shops SET name = ? WHERE email = ?';
var updateZipcodeWhereEmail = 'UPDATE businesses.coffee_shops SET zip_code = ? WHERE email = ?';
var updatePhonenumberWhereEmail = 'UPDATE businesses.coffee_shops SET phone = ? WHERE email = ?';
var updateCoffeeShopFullInfo = 'UPDATE businesses.coffee_shops SET name = ?, zip_code = ?, phone = ?, state = ?, city = ? WHERE email = ?';
var deleteCoffeeShop = 'DELETE from businesses.coffee_shops WHERE email = ?';

// GET all coffee shops
coffeeShopRouter.get('/', function(req, res, next) {
  client.execute(queryCoffeeShops, [], {prepare: true}, function(err, coffeeShops) {
    if(err) {
      debug(err);
      return next(err);
    } else {
      return res.status(200).json(coffeeShops.rows)
    };
  });    
});

// GET coffee shop by id
coffeeShopRouter.get('/:id', function(req, res, next){
  client.execute(queryOneCoffeeShop, {id: req.params.id}, {prepare: true}, function(err, coffeeShop){
    if(err) {
      debug(err);
      next(err);
    } else {
      return res.status(200).json(coffeeShop.rows)
    };
  });
});

// POST new coffee shop
coffeeShopRouter.post('/addcoffeeshop', function(req, res, next) {
  var params = {
    name: req.body.name.toLowerCase(),
    zip_code: req.body.zip_code,
    phone: req.body.phone, 
    state: req.body.state.toLowerCase(), 
    city: req.body.city.toLowerCase(), 
    email: req.body.email.toLowerCase(), 
    id: uuid
  };
  client.execute(createNewCoffeeShop, params, {prepare: true}, function(err, result){
		if (err) {
			debug(err);
			next(err);
		} else {
			return res.json(result);
		};
	});
});

// UPDATE coffee shop name and zip_code
coffeeShopRouter.put('/update/nameandzip', function(req, res, next) {
	// Reject empty req.body values
	for (var i in req.body){
		if (req.body[i] === '') {
		  return res.status(404).json({message: 'Form fields may not be left blank.'});
		};
	};
  
  var query = [
      { 
        query: updateNameWhereEmail,
        params:[req.body.name.toLowerCase(), req.body.email.toLowerCase()]
      },
      {
        query: updateZipcodeWhereEmail,
        params: [req.body.zip_code, req.body.email.toLowerCase()]
      }
   ];
  
	client.batch(query, {prepare: true}, function(err, result){
		if (err) {
			debug(err);
			next(err);
		} else {
		  return res.json(result);
		};
	});
});

// UPDATE  coffee shop full info by email
coffeeShopRouter.put('/update/info/:id', function(req, res, next) {
	// Reject empty req.body values
	for (var i in req.body){
		if (req.body[i] === '') {
			 return res.status(404).json({message: 'Form fields may not be left blank.'});
			 next();
		};
	};
  
  var query = [
    {
      query: updateCoffeeShopFullInfo,
      params:[req.body.name.toLowerCase(), req.body.zip, req.body.phone, req.body.state.toLowerCase(), req.body.city.toLowerCase(), req.body.email.toLowerCase()]
    }
  ];

	client.batch(query, {prepare: true}, function(err, result){
		if (err) {
			debug(err);
			next(err);
		} else {
		  return res.json(result);
		};
	});
});

// DELETE one coffee shop
coffeeShopRouter.delete('/removecoffeeshop', function(req, res, next) {
	client.execute(deleteCoffeeShop, [req.body.email], {prepare: true}, function(err, result){
		if (err) {
			debug(err);
			next(err);
		} else {
		  return res.json(result);
		};
	});
});
//
module.exports = coffeeShopRouter;
