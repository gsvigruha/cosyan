'use strict';

angular.module('cosyan').controller(
    'MetaCtrl',
    function($scope, $http, $document) {
      $scope.tableOpened = {};

      $scope.pickTable = function(tableName) {
        $scope.activeTable = $scope.data.tables[tableName];
      };

      $scope.tableLoc = {};

      $scope.loadTables = function() {
        $http.get("/cosyan/admin", {
          params : {
            token : sessionStorage.getItem('token')
          }
        }).then(function(response) {
          $scope.data = response.data;
          if ($scope.activeTable) {
            $scope.pickTable($scope.activeTable.name);
          }
        });
      };

      $scope.runQuery = function(query) {
        $http.get("/cosyan/sql", {
          params : {
            sql : query,
            token : sessionStorage.getItem('token')
          }
        }).then(function success(response) {
          $scope.$error = undefined;
          $scope.loadTables();
        }, function error(response) {
          $scope.$error = response.data.error;
        });
      };

      $scope.dropConstraint = function(name) {
        var query = 'alter table ' + $scope.activeTable.name
            + ' drop constraint ' + name + ';';
        $scope.runQuery(query);
      };

      $scope.dropColumn = function(name) {
        var query = 'alter table ' + $scope.activeTable.name + ' drop ' + name
            + ';';
        $scope.runQuery(query);
      };

      $scope.dropAggRef = function(name) {
        var query = 'alter table ' + $scope.activeTable.name + ' drop aggref '
            + name + ';';
        $scope.runQuery(query);
      };

      $scope.newColumn = {
        name : '',
        type : '',
        notnull : false,
        unique : false,
        indexed : false,
        immutable : false
      };
      $scope.addColumn = function() {
        var query = 'alter table ' + $scope.activeTable.name + ' add '
            + $scope.newColumn.name + ' ' + $scope.newColumn.type;
        if ($scope.newColumn.unique) {
          query = query + ' unique';
        }
        if ($scope.newColumn.notnull) {
          query = query + ' not null';
        }
        if ($scope.newColumn.immutable) {
          query = query + ' immutable';
        }
        query = query + ');';
        $scope.runQuery(query);
      };

      $scope.newFK = {
        name : '',
        revName : '',
        column : '',
        refTable : ''
      };
      $scope.addFK = function() {
        var query = 'alter table ' + $scope.activeTable.name
            + ' add constraint ' + $scope.newFK.name + ' foreign key ('
            + $scope.newFK.column + ') references ' + $scope.newFK.refTable
            + ' reverse ' + $scope.newFK.revName + ';';
        $scope.runQuery(query);
      };

      $scope.createNewAggRef = function() {
        $scope.newAggRef = {
          name : '',
          expr : ''
        };
      };
      $scope.createNewAggRef();
      $scope.addAggRef = function() {
        var query = 'alter table ' + $scope.activeTable.name + ' add aggref '
            + $scope.newAggRef.name + ' (' + $scope.newAggRef.expr + ');';
        $scope.runQuery(query);
        $scope.createNewAggRef();
      };

      $scope.copyAggRef = function(ref) {
        $scope.newAggRef.name = ref.name;
        $scope.newAggRef.expr = ref.expr;
      };

      $scope.createNewRule = function() {
        $scope.newRule = {
          name : '',
          expr : ''
        };
      };
      $scope.createNewRule();
      $scope.addRule = function() {
        var query = 'alter table ' + $scope.activeTable.name
            + ' add constraint ' + $scope.newRule.name + ' check ('
            + $scope.newRule.expr + ');';
        $scope.runQuery(query);
        $scope.createNewRule();
      };

      $scope.copyRule = function(rule) {
        $scope.newRule.name = rule.name;
        $scope.newRule.expr = rule.expr;
      };

      $scope.loadTables();
    });