var GoogleSpreadsheet = require('google-spreadsheet');
var async = require('async');
var geocoder = require('node-geocoder');
var geoJSON = require('geojson');
var config = require('./config');
var fs = require('fs');
var _ = require('lodash');
const firebase = require('firebase-admin');
var nconf = require('nconf');

const firebaseConfig = nconf.argv().env()
    .file(`${__dirname}/firebase.credentials.json`)
    .get();

// spreadsheet key is the long id in the sheets URL 
var doc = new GoogleSpreadsheet('1lxfgJTb25hCSATgFzpSYR5pXBhuaevs6nTR0PCyBymk');
var worksheet;
var formWorksheet;
var modelWorksheet;

var geocode_options = {
    provider: 'google',
    httpAdapter: 'https', 
    apiKey: config.geocoder_api_key,
    formatter: null         
};

var playgrounds = [];
// cache to prevent redundant calls, to preserve the daily google geocoding quota
var failureCache = [];

var formResponsesById = {};
var formColumns = [];
var model = [];

function loadPlaygrounds(callback) {
    async.series([
        function setAuth(step) {
             var creds = {
                client_email: config.service_account_email,
                private_key: config.service_account_key
            };
            doc.useServiceAccountAuth(creds, step);
        },
        function getInfoAndWorksheets(step) {
            doc.getInfo(function(err, info) {
                console.log('Loaded doc: '+info.title+' by '+info.author.email);
                worksheet = _.filter(info.worksheets, sheet => sheet.title === 'database')[0];
                console.log('sheet: '+worksheet.title+' '+worksheet.rowCount+'x'+worksheet.colCount);
                formWorksheet = _.filter(info.worksheets, sheet => sheet.title === 'form-responses')[0];
                console.log('sheet: '+formWorksheet.title+' '+formWorksheet.rowCount+'x'+formWorksheet.colCount);
                modelWorksheet = _.filter(info.worksheets, sheet => sheet.title === 'model')[0];
                console.log('sheet: '+modelWorksheet.title+' '+modelWorksheet.rowCount+'x'+modelWorksheet.colCount);
                step();
            });
        },
        function loadRows(step) {
            worksheet.getRows({offset: 1, limit: 6}, (err, rows) => {
                console.log(`Read ${rows.length} playgrounds rows`);
                rows = _.filter(rows, row => row['addressdescription'].length > 0);
                console.log(`${rows.length} non empty rows`);
                _.each(rows, row => playgrounds.push(row));
                step();
            });
        },
        function loadFormRows(step) {
            formWorksheet.getRows({offset: 1}, (err, rows) => {
                console.log(`${rows.length} non empty form response rows`);
                _.each(rows, row => {
                    if (formResponsesById[row.id]) {
                        formResponsesById[row.id].push(row);
                    } else {
                        formResponsesById[row.id] = [ row ];
                    }
                });
                formColumns = Object.keys(rows[0]).filter(v => ["_xml", "id", "app:edited", "_links", "save", "del", "timestamp"].indexOf(v) === -1);
                step();
            });
        },
        function loadModelRows(step) {
            modelWorksheet.getRows({offset: 1}, (err, rows) => {
                console.log(`read ${rows.length} form model rows`);

                _.each(rows, row => {
                    model.push({name: row['name'], type: row['type'], label: row['label'], values: row['values'], icons: row['icons']});
                });
                step();
            });
        },
        function returnResult(step) {
            callback();
            step();
        }
    ]);
}

function lookupAddresses1(callback) {
    lookup(cleanAddressStage1, callback, true);
}

function lookupAddresses2(callback) {
    lookup(cleanAddressStage2, callback);
}

function lookupAddresses3(callback) {
    lookup(cleanAddressStage3, callback);
}

function lookupAddresses4(callback) {
    lookup(cleanAddressStage4, callback);
}

function lookupAddresses5(callback) {
    lookup(cleanAddressStage5, callback);
}

function lookupAddresses6(callback) {
    lookup(cleanAddressStage6, callback);
}

function lookup(cleaner, callback, lookupAll) {
    var geo = geocoder(geocode_options);
    var pending = _.filter(playgrounds, playground => !playground.lat);
    console.log(`pending rows: ${pending.length}`);
    var newlyGeocoded = 0;
    async.forEachLimit(pending, 10, function(playground, step) {
        var cleanAddress = cleaner(playground.addressdescription);
        if (failureCache.indexOf(cleanAddress) > -1) {
            step();
            return;
        }
        if (lookupAll || cleanAddress != playground.addressdescription) {    
            geo.geocode(cleanAddress + ' ' + config.city, (err, res) => {
                if (err) {
                    console.log(`error looking up: ${err}`);
                    step();
                    return;
                }
                if (res && res.length > 0) {
                    var geocode = res[0];
                    if (geocode.extra && geocode.extra.googlePlaceId && config.filteredPlaceIds.indexOf(geocode.extra.googlePlaceId) > -1) {
                        console.log(`filtering out for ${cleanAddress}, filtered place id`);
                        failureCache.push(cleanAddress);
                        step();
                        return;
                    }
                    if (geocode.city && config.allowedCities.indexOf(geocode.city) == -1) {
                        console.log(`filtering out for ${cleanAddress}, wrong city ${geocode.city}`);
                        failureCache.push(cleanAddress);
                        step();
                        return;
                    }
                    playground.long = geocode.longitude;
                    playground.lat = geocode.latitude;
                    playground.address = geocode.formattedAddress;
                    playground.locatedaddress = cleanAddress;
                    newlyGeocoded+=1;
                    //console.log(`${playground.name}: ${playground.locatedaddress}`);
                    playground.save(step);   
                }
                else {
                    failureCache.push(cleanAddress);
                    step();    
                }
            });
        }
        else {
            //console.log(`skipping ${cleanAddress}, no changes`);
            step();
        }
    }, function(err) {
        if (err) {
            console.log(`error: ${err}`);
        }
        console.log(`newly geocoded:${newlyGeocoded}`);
        callback();
    });    
}

function report(callback) {
    var notGeoCoded = _.filter(playgrounds, playground => !playground.long);
    var geoCoded = _.filter(playgrounds, playground => playground.long);
    console.log(`total: ${playgrounds.length}, geocoded: ${geoCoded.length}, not geocoded: ${notGeoCoded.length}`);
    callback();
}

function exportGeoJSON(callback) {
    var geoCoded = _.filter(playgrounds, playground => playground.long);
    var json = geoJSON.parse(geoCoded, {Point: ['lat', 'long'], include:['id', 'name', 'neighborhood', 'long', 'lat', 'address', 'addressdescription', 'park', 'playground'].concat(formColumns)});

    console.log('Writing to firebase...');
    let app = firebase.initializeApp({
        credential: firebase.credential.cert(firebaseConfig),
        databaseURL: 'https://playgrounds-f2f0d.firebaseio.com'
    });
    Promise.all([
        firebase.database(app).ref('/public/playgrounds').set(JSON.parse(JSON.stringify(json))),
        firebase.database(app).ref('/public/model').set(JSON.parse(JSON.stringify(model)))
    ]).then(() => {
        console.log('Done writing to firebase.');
        app.delete();
        callback();
    }, e => {
        e && console.error(e);
        app.delete();
        callback();
    });
}

function cleanAddressStage1(addr) {
    return addr.replace("מס'", '');
}

function cleanAddressStage2(addr) {
    addr = cleanAddressStage1(addr);
    return addr.replace('מול', '');
}

function cleanAddressStage3(addr) {
    addr = cleanAddressStage2(addr);
    return addr.replace('+', ' פינת ');
}

function cleanAddressStage4(addr) {
    addr = cleanAddressStage2(addr);
    if (addr.indexOf('+') > 0) {
        addr = addr.substring(0, addr.indexOf('+'));
    }
    return addr;
}

function cleanAddressStage5(addr) {
    addr = cleanAddressStage2(addr);
    if (addr.indexOf('-') > 0) {
        addr = addr.substring(0, addr.indexOf('-'));
    }
    return addr;
}

function cleanAddressStage6(addr) {
    addr = cleanAddressStage2(addr);
    addr = addr.replace('בית', '');
    addr = addr.replace('בלוק', '');
    addr = addr.replace('הבניין', '');
    addr = addr.replace('הבנין', '');
    addr = addr.replace('בניין', '');
    addr = addr.replace('בנין', '');
    addr = addr.replace('ע״י', '');
    addr = addr.replace('על יד', '');
    addr = addr.replace('ליד', '');
    addr = addr.replace('מאחורי', '');
    return addr;
}

function aggregateFormResponses(callback) {
    _.each(playgrounds, playground => {
        let responses = formResponsesById[playground.id];
        if (!responses || responses.length === 0) return;
        let lastResponse = responses.pop();

        if (!lastResponse) {
            console.log('OH NO, WHAT?!');
            console.log(playground);
            console.log(responses);
        }

        for (let column of formColumns) {
            playground[column] = lastResponse[column];
        }
    });
    callback();
}

function saveFailedAddresses(callback) {
    var failed = _.filter(playgrounds, playground => !playground.long && playground.attempted);
    async.eachLimit(failed, 10, (playground, step) => {
        playground.lastsearchedaddress = playground.addressdescription;
        playground.save(step);
    });
    callback();
}

function start() {
    return new Promise((resolve, reject) => {
        async.series([loadPlaygrounds,
            lookupAddresses1, report,
            lookupAddresses2, report,
            lookupAddresses3, report,
            lookupAddresses4, report,
            lookupAddresses5, report,
            lookupAddresses6, report,
            saveFailedAddresses,
            aggregateFormResponses,
            exportGeoJSON, () => resolve()]);
    });

}

module.exports = start;

