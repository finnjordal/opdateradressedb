var fs = require('fs')
  , util= require('util')
  , mongo= require('mongodb');

var Server= mongo.Server
  , Db= mongo.Db
  , BSON= mongo.BSONPure;

var server= new Server('localhost', 27017, {auto_reconnect: true})
  , db= new Db('adressedb', server, {safe: false});

db.open(function(err,db) {
  if (err) {
    console.log('Database ikke åbnet');
    return;
  }
  db.collection('adgangsadresser', function(err, adgangsadresser) { 
    if (err) {
      console.log('Collection adgangsadresser ikke selected');
      return;
    }
    db.collection('adresser', function(err, adresser) { 
      if (err) {
        console.log('Collection ikke oprettet');
        return;
      }
      adgangsadresser.insert({a: "abe"}, {safe: false}, function(err, object) {
        var cursor= adresser.find({"adgangsadresse.id":"39ece5fb-ef0c-0ee2-e044-0003ba298018"});
        cursor.each(function(err, adresse){
          if (err) {
            console.log('fejl i each');
            return;
          }
          if (adresse == null) return;
          console.log("adresse:"+ util.inspect(adresse)); 
          adresse.vej= "Rødkildevej";
          adresse.husnr= "7";
          adresse.postnummer= {nr:"2400", navn:"København NV"};
          adresse.kommunekode= "101";
          adresser.update({"adgangsadresse.id":adgangsadresse.id}, {$set: adresse}, {safe:true, multi: true}, function(err, count) {
            if (err) {
              console.log('adresse ikke opdateret');
            }
            else {          
              console.log('adresse opdateret (%s)',count);
            }
          }); 
        });
      })
    });
  });
});