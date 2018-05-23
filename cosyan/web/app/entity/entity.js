'use strict';

angular.module('cosyan')
.controller('EntityCtrl', function($scope, $http) {
  $scope.load = function() {
    $http.get("/cosyan/entityMeta", {
      params: { sql: $scope.query }
    }).then(function success(response) {
      $scope.data = response.data;
      $scope.$error = undefined;
    }, function error(response) {
      $scope.meta = undefined;
      $scope.$error = response.data.error;
    });
  };
  
  $scope.switchEntity = function(name) {
    for (var i = 0; i < $scope.data.entities.length; i++) {
      if ($scope.data.entities[i].name == name) {
        $scope.activeEntity = $scope.data.entities[i];
      }
    }
  };
  
  $scope.searchFields = {};
  $scope.searchEntity = function() {
    var params = { table: $scope.activeEntity.name };
    for (var field in $scope.searchFields) {
      params['filter_' + field] = $scope.searchFields[field];
    }
    $http.get("/cosyan/searchEntity", {
      params: params
    }).then(function success(response) {
      $scope.entityList = response.data.result[0];
      $scope.$error = undefined;
    }, function error(response) {
      $scope.entityList = undefined;
      $scope.$error = response.data.error;
    });
  };
  
  $scope.openEntity = function(table, id) {
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
  
  $scope.deleteEntity = function(table, id) {
    $http.get("/cosyan/deleteEntity", {
      params: { table: table, id: id }
    }).then(function reload(response) {
      $scope.load();
    });
  };
  
  $scope.load();
});