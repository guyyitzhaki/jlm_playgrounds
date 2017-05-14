var express = require('express');
var app = express();

app.set('port', (process.env.PORT || 5000));

app.get('/', (request, response) => response.sendFile(__dirname + '/index.html'));

app.get('/trigger', (request, response) => response.send('ok'));

app.listen(app.get('port'), () => console.log(`server running on port ${app.get('port')}`));



