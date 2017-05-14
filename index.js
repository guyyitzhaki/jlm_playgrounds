var express = require('express');
var app = express();
var lookup = require('./lookup');

app.set('port', (process.env.PORT || 5000));

app.get('/', (request, response) => response.sendFile(__dirname + '/index.html'));

app.get('/trigger', (request, response) => {
    console.log('Starting lookup...');
    lookup().then(() => response.send('Done')).catch(e => response.send('Error', e));
});

app.use((err, req, res, next) => {
    if (res.headersSent) {
        return next(err);
    }
    console.error(err);
    res.status(500).send(err.stack || err)
});

app.listen(app.get('port'), () => console.log(`server running on port ${app.get('port')}`));



