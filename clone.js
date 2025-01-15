var argv = require('minimist')(process.argv.slice(2));
var prompt = require('prompt');
var merge = require('merge');
var uuidv1 = require('uuid/v1');

// Gather parameters
var host = (argv['h'] || argv['host'])
if (!host) {
  console.log("Host is required");
  process.exit(1);
}

var user = (argv['u'] || argv['user']);
var sessionId = (argv['s'] || argv['session']);
if (!user && !sessionId) {
  console.log('User or Session is required');
  process.exit(1);
}

// Clone type
var type = (argv['t'] || argv['type'])
if (!(type === 'question') && !(type === 'collection') && !(type === 'dashboard')) {
  console.log("Type must be one of [question, collection, dashboard]");
  process.exit(1);
}

// ID
var id = (argv['i'] || argv['id'])
if (!id) {
  console.log("ID is required");
  process.exit(1);
}

if (typeof id === 'string') {
  id = id.split(','); // Split by commas to handle multiple IDs
  console.log(id)
}

// Collection ID
var targetCollection = (argv['c'] || argv['collection']);

if(typeof targetCollection === 'string'){
  targetCollection = targetCollection.split(',')
  console.log(targetCollection)
}

// Target DB
var targetDB = (argv['d'] || argv['database']);
if (!targetDB) {
  console.log("Database ID is required");
  process.exit(1);
}

if(typeof targetDB === 'string'){
  targetDB = targetDB.split(',')
  console.log(targetDB)
}

// Prompt for password
var password = null;
if (!sessionId) {
  prompt.start();
  prompt.get({properties: 
    {password: {hidden: true, message: "Metabase password", required: true}}}, 
    function (err, result) {
      var api = require('./src/metabase_api')({
        host: host,
        user: user,
        password: result.password
      });
      console.log("Connecting to '" + host + "'");

      if (sessionId) {
        doClone(api, {type: type, id: id, targetDB: targetDB, 
          targetCollection: targetCollection});
      } else {
        api.login().then(function() {
          doClone(api, {type: type, id: id, targetDB: targetDB, 
            targetCollection: targetCollection});
        }).catch(console.error);
      }
    });
  // Or use supplied session
} else {
  var api = require('./src/metabase_api')({
    host: host,
    sessionId: sessionId
  });
  doClone(api, {type: type, id: id, targetDB: targetDB, 
    targetCollection: targetCollection});
}




function doClone(api, params) {
  console.log("Cloning " + params.type + 
    " id [" + params.id + "] to DB [" + 
    params.targetDB + "] and to collection [" +
    params.targetCollection + "]");
  if (params.type === 'question') {
    cloneQuestion(api, params);
  } else if (params.type === 'collection') {
    cloneCollection(api, params);
  } else if (params.type === 'dashboard') {
    cloneDashboard(api, params);
  }
}

// Copies the questions in the collection to the target collection
// Params:
//  id: Collection to clone
//  targetDB: ID of DB to target
//  targetCollection: ID of collection to copy questions to
async function cloneCollection(api, params) {
  console.log("Cloning collection ID " + params.id);
  try {
    var collectionItems = await api.getCollectionItems(params.id);
    collectionItems = collectionItems.data
    console.log(collectionItems)
    for (var i = 0; i < collectionItems.length; i++) {
      if (collectionItems[i].model === 'card') {
        cloneQuestion(api, {
          id: collectionItems[i].id, 
          targetDB: targetDB, 
          targetCollection: targetCollection
        });
      }
    }
  } catch(e) {
    console.error(e);
  }
}

async function cloneQuestion(api, params) {
  console.log("Cloning question ID " + params.id);
  try {
    if(Array.isArray(params.id)){
      for(var i = 0; i < params.id.length; i++){
        var question = await api.getQuestion(params.id[i]);
        var sourceFields = await api.getFields(question.database_id);
        var targetFields = await api.getFields(params.targetDB);

        var newQuestion = await questionPropertiesTo(api, question, sourceFields, targetFields, 
          params.targetDB, params.targetCollection);
        var result = api.postQuestion(newQuestion); 
      }
      return;
    }
    var question = await api.getQuestion(params.id);
    var sourceFields = await api.getFields(question.database_id);
    var targetFields = await api.getFields(params.targetDB);

    var newQuestion = await questionPropertiesTo(api, question, sourceFields, targetFields, 
      params.targetDB, params.targetCollection);
    var result = api.postQuestion(newQuestion);
  } catch(e) {
    console.error("Error cloning question ID " + params.id, e);
  }
}

// Clone a dashboard, creating a new one from the structure of the old one,
// and adds the cards with the same names from the target dashboard.  The questions
// with the same names must be present in the source and target databases.
// params:
//  id: Dashboard to clone
//  targetCollection: ID to put new dashboard in
async function cloneDashboard(api, params) {
  console.log("Cloning dashboard ID " + params.id);
  try {

    // get source dashboard
    var dashboard = await api.getDashboard(params.id);
    console.log('Start Point dashcards', dashboard.dashcards)
    var newDashboard = {
      name: dashboard.name,
      description: dashboard.description,
      parameters: dashboard.parameters,
      collection_id: params.targetCollection
    }
    var savedDashboard = await api.postDashboard(newDashboard);

    const tabs = dashboard.tabs.map(tab => {
      return {
        id: savedDashboard.id + tab.id,
        name: tab.name
      }
    })

    const textCards = dashboard.dashcards.filter(card => card.card_id === null)

    const addedTabs = await api.postDashboardTabs(savedDashboard.id, tabs);

    // Create a map for quick lookup of old tab IDs to new tab IDs
    const tabIdMap = dashboard.tabs.reduce((map, tab, index) => {
      map[tab.id] = addedTabs.tabs[index].id; // Map old tab ID to new tab ID based on index
      return map;
    }, {});

    // Find questions in target DB with same names, create dashboard cards
    var dbItems = await api.getCollectionItems(params.targetCollection); 
    dashboard.ordered_cards = dashboard.dashcards;
    var newCards = [];
    for (var i = 0; i < dashboard.dashcards.length; i++) {
      var cardDef = dashboard.dashcards[i];
      var card = cardDef.card;
      var parameter_mappings = dashboard.dashcards[i].parameter_mappings;
      var targetCard = findCard(card.name, dbItems.data);

      if (!targetCard?.id) {
        // throw "Unable to find target card " + card.name;
      } else {

        for (var p = 0; p < parameter_mappings.length; p++) {
          parameter_mappings[p].card_id = targetCard.id;
          if (parameter_mappings[p]?.target.length > 0) {
            var sourceFields = await api.getFields(card.database_id);
            var targetFields = await api.getFields(targetDB);
            parameter_mappings[p].target = mapFieldIds(parameter_mappings[p].target, sourceFields, targetFields);
          }
        }

        // Get the old tab ID from the card and map it to the new tab ID using the tabIdMap
        const oldTabId = cardDef.dashboard_tab_id; // Get the old tab ID from the card
        const newTabId = tabIdMap[oldTabId] || ( addedTabs?.tabs[0]?.id ? addedTabs?.tabs[0]?.id : null); // Default to the first tab if no match

        //console.log('cardDef ----------------------------------------------------------------------------------------' , cardDef)
        newCards.push({
          id: savedDashboard.id,
          card_id: targetCard.id,
          size_x: cardDef.size_x,
          size_y: cardDef.size_y,
          row: cardDef.row,
          col: cardDef.col,
          series: cardDef.series,
          parameter_mappings: parameter_mappings,
          visualization_settings: cardDef.visualization_settings,
          dashboard_tab_id: newTabId
        });
      }
    }

    for(var i = 0; i < textCards.length; i++){
      var cardDef = textCards[i];
      console.log('New Card Def', cardDef)

      // Get the old tab ID from the text card and map it to the new tab ID using the tabIdMap
      const oldTabId = cardDef.dashboard_tab_id; // Get the old tab ID from the text card
      const newTabId = tabIdMap[oldTabId] || addedTabs.tabs[0].id; // Default to the first tab if no match

      newCards.push({
        id: savedDashboard.id,
        size_x: cardDef.size_x,
        size_y: cardDef.size_y,
        row: cardDef.row,
        col: cardDef.col,
        series: cardDef.series,
        visualization_settings: cardDef.visualization_settings,
        dashboard_tab_id: newTabId
      });
    }

    // and put cards to dashboard
    for (var c = 0; c < newCards.length; c++) {
      if (newCards[c]) {
        var cardToSave = newCards[c];
        var savedCard = await api.postDashboardCard(savedDashboard.id, cardToSave);
      }
      console.log(`Done ${c+1} of ${newCards.length}`)
    }
  } catch(e) {
    console.error(e);
  }
}

function mapTableId(tableId, allTables, targetDbId) {
  // Find the source table by its table_id
  const sourceTable = allTables.find(table => table.id == tableId);

  if (!sourceTable) {
    throw new Error(`Source table with table_id not found`);
  }

  // Find the corresponding target table with the same name, but in a different database
  const targetTable = allTables.find(table => table.name == sourceTable.name && table.db_id == targetDbId);

  if (!targetTable) {
    throw new Error(`Target table for not found in a different database.`);
  }

  // Return the target table_id
  return targetTable.id;
}

function findFieldById(fieldsArray, fieldId) {
  return fieldsArray.find(field => field.id == fieldId)
}

//[
//    {
//        "id": 3317,
//        "name": "quantity",
//        "display_name": "Quantity",
//        "base_type": "type/Float",
//        "semantic_type": null,
//        "table_name": "entitlement_deductions",
//        "schema": "badd819e_8fd3_41a5_819d_053762db8fac"
//    }]

function findFieldByName(fieldsArray, fieldName, tableName) {
  return fieldsArray.find(field => field.name == fieldName && field.table_name == tableName)
}

function mapFieldIds(fieldRefs, source_fields, target_fields) {
  return fieldRefs.map(fieldRef => {
    if (fieldRef[0] === 'field' && fieldRef[1]) {
      // Field reference: Map the field ID
      const fieldId = fieldRef[1];
      const sourceField = findFieldById(source_fields, fieldId);  // Find field in source table
      if (sourceField) {
        // Find corresponding field in target table
        const targetField = findFieldByName(target_fields, sourceField.name, sourceField.table_name);
        if (targetField) {
          // Map fieldRef to target field ID
          fieldRef[1] = targetField.id;
        }
      }
    } else if (Array.isArray(fieldRef)) {
      // Recursively map nested conditions inside logical operators like 'and', 'or', '='
      // For example, "and", ["=", ["field", 410], 2]
      fieldRef = mapFieldIds(fieldRef, source_fields, target_fields);
    }
    return fieldRef;
  });
}

// Function to recursively map expressions
function mapExpressions(expressions, source_fields, target_fields) {
  const mappedExpressions = {};

  Object.keys(expressions).forEach(expressionKey => {
    let expression = expressions[expressionKey];

    if (Array.isArray(expression)) {
      // Map the expression if it contains field references
      expression = expression.map(item => {
        if (Array.isArray(item)) {
          // If it's an array, it could be a field reference, so map it
          return mapFieldIds(item, source_fields, target_fields);
        }
        return item;
      });
    }

    // After mapping all nested items, assign it back to the expression
    mappedExpressions[expressionKey] = expression;
  });

  return mappedExpressions;
}

async function questionPropertiesTo(api, original, source_fields, target_fields, 
  database_id, collection_id) {

  var dataset_query = merge(original.dataset_query, {database: database_id});
  if (dataset_query.type === 'native') {
    dataset_query.native['template-tags'] = toTargetTemplateTags(
      dataset_query.native['template-tags'], source_fields, target_fields);
  }

  var tables = await api.getTables();

  console.log(dataset_query?.query?.['source-table'], "Source Table")

  const queryTargetTableId = dataset_query?.query?.['source-table'] ? mapTableId(dataset_query.query['source-table'], tables, database_id) : null;
  // Handle joins (if any)
  if (dataset_query?.query?.joins) {
    for (let join of dataset_query.query.joins) {
      // Map each join's source-table to the corresponding target table
      const joinSourceTableId = mapTableId(join['source-table'], tables, database_id);
      join['source-table'] = joinSourceTableId;

      if(join?.condition?.length > 0){
        join['condition'] = mapFieldIds(join['condition'], source_fields, target_fields);
      }
    }

  }

  if(dataset_query?.query?.['source-table']){
    dataset_query.query['source-table'] = queryTargetTableId;
  } 

  // Map field IDs in the query (aggregation, breakout, filter)
  if (dataset_query?.query?.aggregation) {
    dataset_query.query.aggregation = mapFieldIds(dataset_query.query.aggregation, source_fields, target_fields);
  }

  if (dataset_query?.query?.breakout) {
    dataset_query.query.breakout = mapFieldIds(dataset_query.query.breakout, source_fields, target_fields);
  }

  if (dataset_query?.query?.['order-by']) {
    dataset_query.query['order-by'] = mapFieldIds(dataset_query.query['order-by'], source_fields, target_fields);
  }

  if (dataset_query?.query?.filter) {
    dataset_query.query.filter = mapFieldIds(dataset_query.query.filter, source_fields, target_fields);
  }

  // Handle expressions field mapping
  if (dataset_query?.query?.expressions) {
    dataset_query.query.expressions = mapExpressions(dataset_query.query.expressions, source_fields, target_fields);
  }

  return {
    name: original.name,
    query_type: original.query_type,
    description: original.description,
    database_id: database_id,
    table_id: queryTargetTableId,
    collection_id: collection_id,
    result_metadata: original.result_metadata,
    dataset_query: dataset_query,
    display: original.display,
    visualization_settings: original.visualization_settings
  }
}

function toTargetTemplateTags(template_tags, old_fields, target_fields) {
  var target = {};
  for (var key in template_tags) {
    target[key] = merge(template_tags[key], {
      id: uuidv1()
    });
    if (target[key].dimension) {
      // Find field ID in old fields
      var fieldId = target[key].dimension[1];
      var fieldName = null;
      var tableName = null;
      for (var i = 0; i < old_fields.length; i++) {
        if (fieldId == old_fields[i].id) {
          fieldName = old_fields[i].name;
          tableName = old_fields[i].table_name;
          break;
        }
      }
      if (!fieldName || !tableName) throw "Can't find field ID in source";

      var targetFieldId = null;
      for (var i = 0; i < target_fields.length; i++) {
        if (target_fields[i].name === fieldName && 
          target_fields[i].table_name === tableName) {
          targetFieldId = target_fields[i].id;
          break;
        }
      }
      if (!targetFieldId) throw "Can't find target field for " + fieldName;

      target[key].dimension = ["field-id", targetFieldId];
    }
  }
  return target;
}


function findCard(name, items) {
  for (var i = 0; i < items.length; i++) {
    if (items[i].model !== 'dashboard' && items[i].name === name) {
      return items[i];
    }
  }
  return null;
}
