'use strict';

angular.module('cosyan')
.controller('EntityCtrl', function($scope, $http, util) {
  $scope.load = function() {
    $http.get("/cosyan/entityMeta").then(function success(response) {
      $scope.data = response.data;
      $scope.$error = undefined;
      $scope.loadedEntity = undefined;
      $scope.entityList = undefined;
    }, function error(response) {
      $scope.meta = undefined;
      $scope.$error = response.data.error;
      $scope.loadedEntity = undefined;
      $scope.entityList = undefined;
    });
  };
  
  $scope.switchEntity = function(name) {
    for (var i = 0; i < $scope.data.entities.length; i++) {
      if ($scope.data.entities[i].name == name) {
        $scope.activeEntity = $scope.data.entities[i];
        $scope.$error = undefined;
        $scope.loadedEntity = undefined;
        $scope.entityList = undefined;
      }
    }
  };
  
  $scope.searchFields = {};
  $scope.searchEntity = function() {
    if (!$scope.activeEntity) {
      return;
    }
    var params = { table: $scope.activeEntity.name };
    for (var field in $scope.searchFields) {
      params['filter_' + field] = $scope.searchFields[field];
    }
    $http.get("/cosyan/searchEntity", {
      params: params
    }).then(function success(response) {
      $scope.entityList = response.data.result[0];
      $scope.$error = undefined;
      $scope.loadedEntity = undefined;
    }, function error(response) {
      $scope.entityList = undefined;
      $scope.$error = response.data.error;
      $scope.loadedEntity = undefined;
    });
  };
  
  $scope.collapse = function() {
    $scope.entityList= undefined;
  };
  
  $scope.openEntity = function(id) {
    var table = $scope.activeEntity.name;
    $http.get("/cosyan/loadEntity", {
      params: { table: table, id: id }
    }).then(function success(response) {
      $scope.loadedEntity = response.data.result[0];
      $scope.$error = undefined;
    }, function error(response) {
      $scope.loadedEntity = undefined;
      $scope.$error = response.data.error;
    });
  };
  
  $scope.newEntity = function() {
    $scope.loadedEntity = util.createNewEntity($scope.activeEntity);
    $scope.$error = undefined;
  };
  
  $scope.deleteEntity = function(id) {
    var table = $scope.activeEntity.name;
    var idName = $scope.activeEntity.fields[0].name;
    var query = 'delete from ' + table + ' where ' + idName + ' = ' + id + ';';
    $http.get("/cosyan/sql", {
      params: { sql: query }
    }).then(function success(response) {
      $scope.searchEntity();
    }, function error(response) {
      $scope.$error = response.data.error;
    });
  };
  
  $scope.load();
});