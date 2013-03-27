var fs = require('fs')
  , util= require('util')
  , mongo= require('mongodb')
  , uuid = require('node-uuid');

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
  var input = fs.createReadStream('C:\\data\\aws\\AddressAccess.csv');
  var output = fs.createWriteStream('adgangsadresselog.txt', {flags: 'w'});
  readLines(input, output, func, db);
});



var active= 0
  , limit= 5000
  , total= 0;
function readLines(input, output, func, db) {
  db.collection('adresser', function(err, adresser) { 
    if (err) {
      console.log('Collection adresser ikke selected');
      return;
    }

    var remaining = ''
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
          func(line, input, output, adresser);
          total++;
          if (++active > limit) {
            input.pause();
            console.log('pause (%s)', active);
          }
        }
        index = remaining.indexOf('\n');
      }
    });

    input.on('end', function() {
      console.log('total: ' + total);
    });
  });
}

function func(data, input, output, adresser) {
  var result= true;
  var fields= data.replace(/"/g,"").split(';');
  var adgangsadresse= {};
  adgangsadresse.id= fields[0];  // AddressAccessIdentifier
  adgangsadresse.version= fields[1]; //VersionId
  adgangsadresse.bygningsnavn= fields[2]; // BuildingName
  adgangsadresse.kommunekode= fields[3]; // MunicipalityCode
  adgangsadresse.vej= {};
  adgangsadresse.vej.kode= fields[4]; // StreetCode
  adgangsadresse.vej.navn= fields[5]; // StreetName
  adgangsadresse.husnr= fields[6]; // StreetBuildingIdentifier
  adgangsadresse.supplerendebynavn= fields[7]; // DistrictSubdivisionIdentifier
  adgangsadresse.postnummer= {};
  adgangsadresse.postnummer.nr= fields[8]; // PostCodeIdentifier
  adgangsadresse.postnummer.navn= fields[9]; // DistrictName
  adgangsadresse.ejerlav= {};
  adgangsadresse.ejerlav.nr= fields[10]; // CadastralDistrictIdentifier
  adgangsadresse.ejerlav.navn= fields[11]; // CadastralDistrictName
  adgangsadresse.landsejerlav= fields[12]; // LandParcelIdentifier
  adgangsadresse.matrikelnr= fields[13]; // MunicipalRealPropertyIdentifier
  adgangsadresse.oprettet= fields[14]; // AddressAccessCreateDate
  adgangsadresse.gyldig= fields[15]; // AddressAccessValidDate
  adgangsadresse.ændret= fields[16]; // AddressAccessChangeDate
  adgangsadresse.etrs89koordinat= {};
  adgangsadresse.etrs89koordinat.øst= fields[17]; // ETRS89utm32Easting
  adgangsadresse.etrs89koordinat.nord= fields[18]; // ETRS89utm32Northing
  adgangsadresse.wgs84koordinat= {};
  adgangsadresse.wgs84koordinat.bredde= fields[19]; // WGS84GeographicLatitude
  adgangsadresse.wgs84koordinat.længde= fields[20]; // WGS84GeographicLongitude
  adgangsadresse.adressepunktnøjagtighed= fields[21]; // AddressCoordinateQualityClassCode
  adgangsadresse.adressepunktkilde= fields[22]; // AddressGeometrySourceCode
  adgangsadresse.adressepunkttekniskstandard= fields[23]; // AddressCoordinateTechnicalStandardCode
  adgangsadresse.adressepunkttekstretning= fields[24]; // AddressTextAngleMeasure
  adgangsadresse.KN100mDK= fields[25]; // GeometryDDKNcell100mText
  adgangsadresse.KN1kmDK= fields[26]; // GeometryDDKNcell1kmText
  adgangsadresse.KN10kmDK= fields[27]; // GeometryDDKNcell10kmText
  adgangsadresse.adressepunktændret= fields[28]; // AddressPointRevisionDateTime
 // console.log('adresse fundet: '+adresse);
 // console.log("adresse:"+ util.inspect(adresse));
  var adresse= {};
  adresse.vej= adgangsadresse.vej;
  adresse.husnr= adgangsadresse.husnr;
  adresse.postnummer= adgangsadresse.postnummer;
  adresse.kommunekode= adgangsadresse.kommunekode;
  adresse.etrs89koordinat= adgangsadresse.etrs89koordinat;
  adresse.wgs84koordinat= adgangsadresse.wgs84koordinat;
  console.log('id: (%s), total:(%s)',adgangsadresse.id,total); 
  adresser.update({"adgangsadresse.id":adgangsadresse.id}, {$set: adresse}, {safe:(total%4000===0), multi: true}, function(err, count) {
    if (--active <= limit) {
      input.resume();
      console.log('resume (%s), total: (%s)',active, total);
    }
    if (err) {
      console.log('adresse ikke opdateret');
      return;
    }
    if (count > 0) {          
      console.log('%s adresser opdateret (%s)',count,total);
      return;
    }
    // adresse.id= uuid.v1(); // AddressSpecificIdentifier
    // adresse.version= adgangsadresse.version; // VersionId
    // adresse.adgangsadresse= {};
    // adresse.adgangsadresse.id= fields[2];   //AddressAccessReference
    // adresse.adgangsadresse.oprettet= adgangsadresse.oprettet; // AddressAccessCreateDate
    // adresse.adgangsadresse.gyldig= adgangsadresse.gyldig; // AddressAccessValidDate
    // adresse.adgangsadresse.ændret= adgangsadresse.ændret; // AddressAccessChangeDate
    // adresse.etage= ''; // FloorIdentifier
    // adresse.dør= ''; // SuiteIdentifier
    // adresser.insert(adresse, {safe: true}, function(err, object) {
    //   if (err) {
    //     console.log('adresse ikke indsat. Fejl %s',err);
    //   }
    //   else {
    //     console.log('adresse indsat');
    //   }
    // });
  }); 
  //console.log('id: '+adresse.id); 
  //console.log('vej: '+util.inspect(adresser));

}

