module.exports = function(params) {
  var Client = require('node-rest-client').Client;
  var client = new Client();

  var host = params.host;
  var user = params.user;
  var password = params.password;
  var sessionId = params.sessionId;

  var analyticsZenskarHost = 'analytics.zenskar.com';
  var analyticsZenskarSesionId = params.analyticsZenskarSesionId;

  function headers() {
    var h = {"Content-Type": "application/json"};
    if (sessionId) h['X-Metabase-Session'] = sessionId;
    return h;
  };

  function analyticsZenskarHeaders(){
    var h = {"Content-Type": "application/json"};
    if (analyticsZenskarSesionId) h['X-Metabase-Session'] = analyticsZenskarSesionId;
    return h;
  }

  var def = {

    login: function() {
      var args = {
        data: {
          username: user,
          password: password
        },
        headers: headers(),
        path: {host: host}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/session`;
        client.post(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            console.log("Using X-Metabase-Session=" + data.id);
            sessionId = data.id;
            resolve(data);
          }
        });
      });
    },

    loginAnalyticsZenskar: function() {
      var args = {
        data: {
          username: 'admin@zenskar.com',
          password: 'i#6kGf^gNU£1Q52;;OM'
        },
        headers: headers(),
        path: {host: analyticsZenskarHost}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${analyticsZenskarHost}/api/session`;
        client.post(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            console.log("Using Analytics Zenskar X-Metabase-Session=" + data.id);
            analyticsZenskarSesionId = data.id;
            resolve(data);
          }
        });
      });
    },

    getQuestion: function(id) {
      var args = {
        headers: headers(),
        path: {host: host, id: id}
      }
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/card/${id}`;
        client.get(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    postQuestion: function(question) {
      var args = {
        data: question,
        headers: headers(),
        path: {host: host}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/card`;
        client.post(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    getFields: function(database_id) {
      var args = {
        headers: headers(),
        path: {host: host, id: database_id}
      }
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/database/${database_id}/fields`;
        client.get(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    getCollectionItems: function(id) {
      var args = {
        headers: headers(),
        path: {host: host, id: id}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/collection/${id}/items`;
        client.get(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    getDashboard: function(id) {
      var args = {
        headers: headers(),
        path: {host: host, id: id}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/dashboard/${id}`;
        client.get(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    postDashboard: function(dashboard) {
      var args = {
        data: dashboard,
        headers: headers(),
        path: {host: host}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/dashboard`;
        client.post(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    postDashboardTabs: function(id, tabs) {
      console.log('Tabs Dashboard id', id ,tabs)
      var args = {
        data: {dashcards : [], tabs : tabs},
        headers: headers(),
        path: {host: host, id: id}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/dashboard/${id}`
        client.put(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },
    
    getTables: function() {
      var args = {
        headers: headers(),
        path: {host: host}
      };
      return new Promise(function(resolve, reject) {
        var path = `https://${host}/api/table`;
        client.get(path, args, function(data, response) {
          if (response.statusCode != 200) {
            reject(response.statusMessage);
          } else {
            resolve(data);
          }
        });
      });
    },

    postDashboardCard: function(id, card) {
  var args = {
    headers: headers(),
    path: { host: host, id: id }
  };

       // Step 1: Fetch current dashboard cards
    var path = `https://${host}/api/dashboard/${id}`;

  return new Promise((resolve, reject) => { 
    
    // Step 2: Now fetch the existing dashboard cards
    client.get(path, args, (data, response) => {
        if (response.statusCode !== 200) {
          return reject(response.statusMessage);
        }

        // Step 3: Append the new card to the existing cards
        var existingCards = data.dashcards || [];
        var existingTabs = data.tabs || []
        existingCards.push(card);

        // Step 4: Send the updated list back to the server
        var updateArgs = {
            data: { dashcards: existingCards, tabs: existingTabs},
          headers: headers(),
          path: { host: host, id: id }
        };
        //console.log('Update arguments', updateArgs);

        client.put(path, updateArgs, (updateData, updateResponse) => {
          if (updateResponse.statusCode !== 200) {
            console.log(updateResponse);
            return reject(updateResponse.statusMessage);
          }
          resolve(updateData);
        });
      });
  });
}

  }

  return def;

}
