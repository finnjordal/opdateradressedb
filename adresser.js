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
  var input = fs.createReadStream('C:\\data\\aws\\Addressspecific.csv');
  var output = fs.createWriteStream('adresselog.txt', {flags: 'w'});
  db.dropCollection('adresser', function(err,result) {
    readLines(input, output, func, db);
    db.close();
  });
});


function readLines(input, output, func, db) {
  db.collection('adresser', function(err, adresser) { 
    if (err) {
      console.log('Collection ikke oprettet');
      return;
    }

    var remaining = ''
      , total= 0
      , first= true;

    input.on('data', function(data) {
      remaining += data;
      var index = remaining.indexOf('\n');
      while (index > -1) {
        var line = remaining.substring(0, index);
        line= line.replace(/(\r\n|\n|\r)/gm,"");
        remaining = remaining.substring(index + 1);
        if (first) {
          first= false;
        }
        else {
          func(line, output, adresser);
          total++;
        }
        index = remaining.indexOf('\n');
      }
    });

    input.on('end', function() {
      if (remaining.length > 0) {
        func(remaining, output);
      }
      console.log('total: ' + total);
    });

  });
}

function func(data, output, adresser) {
  var result= true;
  var fields= data.replace(/"/g,"").split(';');
  var adresse= {};
  adresse.id= fields[0]; // AddressSpecificIdentifier
  adresse.version= fields[1]; // VersionId
  adresse.adgangsadresse= {};
  adresse.adgangsadresse.id= fields[2];   //AddressAccessReference
  adresse.adgangsadresse.oprettet= fields[3]; // AddressAccessCreateDate
  adresse.adgangsadresse.gyldig= fields[4]; // AddressAccessValidDate
  adresse.adgangsadresse.ændret= fields[5]; // AddressAccessChangeDate
  adresse.etage= fields[6]; // FloorIdentifier
  adresse.dør= fields[7]; // SuiteIdentifier
  adresser.insert(adresse, {safe: true}, function(err, object) {
    if (err) {
      console.log('adresse ikke indsat. Fejl %s',err);
    }
  });
}

