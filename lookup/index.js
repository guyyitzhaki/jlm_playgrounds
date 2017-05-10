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
// cache to prevent redundant calls, to preserve the daily google geocoding quota
var failureCache = [];

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
                step();
            });
        },
        function loadRows(step) {
            worksheet.getRows({offset: 1}, (err, rows) => {
                console.log(`Read ${rows.length} rows`);
                rows = _.filter(rows, row => row['addressdescription'].length > 0);
                console.log(`${rows.length} non empty rows`);
                _.each(rows, row => playgrounds.push(row));
                step();
            });
        },
        function returnResult(step) {
            callback();
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

function lookup(cleaner, callback, lookupAll=false) {
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

function saveResults(callback) {
    var modified = _.filter(playgrounds, playground => playground.modified);
    console.log(`modified:${modified.length}`);
    async.each(modified, (playground, step) => {
        console.log(`saving ${playground.name}`);
        playground.save(step);
    });
    console.log('done saving');
    callback();
}

function report(callback) {
    var notGeoCoded = _.filter(playgrounds, playground => !playground.long);
    var geoCoded = _.filter(playgrounds, playground => playground.long);
    console.log(`total: ${playgrounds.length}, geocoded: ${geoCoded.length}, not geocoded: ${notGeoCoded.length} sanity:${notGeoCoded.length + geoCoded.length}` );
    callback();
}

function exportGeoJSON(callback) {
    var geoCoded = _.filter(playgrounds, playground => playground.long);
    var json = geoJSON.parse(geoCoded, {Point: ['lat', 'long'], include:['id', 'name', 'neighborhood', 'long', 'lat', 'address', 'addressdescription', 'park', 'playground']});
    var filename = 'playgrounds.geojson';
    fs.writeFile(filename, JSON.stringify(json), err => {
        console.log(err ? err : `saved ${filename}`);
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

async.series([loadPlaygrounds, 
    lookupAddresses1, report, 
    lookupAddresses2, report, 
    lookupAddresses3, report, 
    lookupAddresses4, report, 
    lookupAddresses5, report, 
    lookupAddresses6, report, 
    exportGeoJSON]);







