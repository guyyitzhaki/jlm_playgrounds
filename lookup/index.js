var GoogleSpreadsheet = require('google-spreadsheet');
var async = require('async');
var geocoder = require('node-geocoder');
var geoJSON = require('geojson');
var config = require('./config');
var fs = require('fs');
var _ = require('lodash');

// spreadsheet key is the long id in the sheets URL 
var doc = new GoogleSpreadsheet('1IXlP0P-QSUuTOJ-XopU2CVSmTiqApWoNvwCb0nn6bF4');
var worksheet;

var geocode_options = {
    provider: 'google',
    httpAdapter: 'https', 
    apiKey: config.geocoder_api_key,
    formatter: null         
};

var playgrounds = [];

function loadPlaygrounds(callback) {
    async.series([
        function setAuth(step) {
             var creds = {
                client_email: config.service_account_email,
                private_key: config.service_account_key
            };
            console.log(creds);
            doc.useServiceAccountAuth(creds, step);
        },
        function getInfoAndWorksheets(step) {
            doc.getInfo(function(err, info) {
                console.log('Loaded doc: '+info.title+' by '+info.author.email);
                worksheet = _.filter(info.worksheets, sheet => sheet.title === 'database')[0];
                console.log('sheet 1: '+worksheet.title+' '+worksheet.rowCount+'x'+worksheet.colCount);
                step();
            });
        },
        function loadRows(step) {
            worksheet.getRows({offset: 1}, (err, rows) => {
                console.log(`Read ${rows.length} rows`);
                rows = _.filter(rows, row => row['addressdescription'].length > 0);
                console.log(`${rows.length} non empty rows`);
                _.each(rows, (row, index) => {
                    playgrounds.push({name: row['name'], address: row['addressdescription'], neighborhood: row['neighborhood'], id: index});
                });
                step();
            });
        },
        function returnResult(step) {
            callback();
        }
    ]);
}

function lookupAddresses(callback) {
    console.log('looking up');
    var geo = geocoder(geocode_options);
    async.forEachLimit(playgrounds, 10, function(playground, step) {
        geo.geocode(cleanAddress(playground.address), function(err, res) {
            if (err) {
                console.log(`error looking up: ${err}`);
                step();
                return;
            }
            if (res && res.length > 0) {
                var geocode = res[0];
                playground.longitude = geocode.longitude;
                playground.latitude = geocode.latitude;
                playground.formattedAddress = geocode.formattedAddress;
                console.log(`${playground.name}: ${playground.formattedAddress}`);
            }
            step();
        });
    }, function(err) {
        if (err) {
            console.log(`error: ${err}`);
        }
        callback();
    });
}
function report(callback) {
    var notGeoCoded = _.filter(playgrounds, playground => !playground.longitude);
    var geoCoded = _.filter(playgrounds, playground => playground.longitude);
    console.log(`total: ${playgrounds.length}, geocoded: ${geoCoded.length}, not geocoded: ${notGeoCoded.length} sanity:${notGeoCoded.length + geoCoded.length}` );
    callback();
}

function exportGeoJSON(callback) {
    var geoCoded = _.filter(playgrounds, playground => playground.longitude);
    var json = geoJSON.parse(geoCoded, {Point: ['latitude', 'longitude']});
    var fname = 'playgrounds.geojson';
    fs.writeFile(fname, 'var playgrounds ='+JSON.stringify(json), err => {
        if(err) {
            console.log(err);
            return;
        }
        console.log(`saved ${fname}`);
        callback();
    });
}

function cleanAddress(addr) {
    addr = addr.replace("מס'", '');
    addr += " ירושלים";
    return addr;
}



async.series([loadPlaygrounds, /*lookupAddresses, report, exportGeoJSON*/]);





