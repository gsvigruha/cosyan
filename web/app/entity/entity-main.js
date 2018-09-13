'use strict';

angular.module('cosyan').controller(
    'EntityCtrl',
    function($scope, $http, util) {
      util.entities(function(e) {
        $scope.entities = e;
      });
      $scope.$error = undefined;
      $scope.loadedEntity = undefined;
      $scope.entityList = undefined;

      $scope.switchEntityType = function(name) {
        for (var i = 0; i < $scope.entities.length; i++) {
          if ($scope.entities[i].name == name) {
            $scope.searchFields = {};
            var aet = $scope.entities[i];
            for ( var j in aet.fields) {
              var field = aet.fields[j];
              if (field.search) {
                $scope.searchFields[field.name] = {
                  value : undefined,
                  type : field.type
                };
              }
            }
            $scope.activeEntityType = aet;
            $scope.$error = undefined;
            $scope.loadedEntity = undefined;
            $scope.entityList = undefined;
            break;
          }
        }
      };

      $scope.unsetSearchField = function(field) {
        $scope.searchFields[field.name].value = undefined;
      };

      $scope.searchFields = {};
      $scope.searchEntity = function() {
        if (!$scope.activeEntityType) {
          return;
        }
        var query = 'select * from ' + $scope.activeEntityType.name;
        var where = [];
        for ( var field in $scope.searchFields) {
          if ($scope.searchFields[field].value) {
            where.push(field + '=' + util.format($scope.searchFields[field]));
          }
        }
        if (where.length > 0) {
          query = query + ' where ' + where.join(' and ');
        }
        query = query + ';';
        $http.get("/cosyan/sql", {
          params : {
            sql : query,
            token : sessionStorage.getItem('token')
          }
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
        $scope.entityList = undefined;
      };

      $scope.openEntity = function(id) {
        var table = $scope.activeEntityType.name;
        $http.get("/cosyan/loadEntity", {
          params : {
            table : table,
            id : id,
            token : sessionStorage.getItem('token')
          }
        }).then(function success(response) {
          $scope.loadedEntity = response.data.result[0];
          $scope.$error = undefined;
        }, function error(response) {
          $scope.loadedEntity = undefined;
          $scope.$error = response.data.error;
        });
      };

      $scope.newEntity = function() {
        $scope.loadedEntity = util.createNewEntity($scope.activeEntityType);
        $scope.$error = undefined;
      };

      $scope.deleteEntity = function(id) {
        var table = $scope.activeEntityType.name;
        var idName = $scope.activeEntityType.fields[0].name;
        var query = 'delete from ' + table + ' where ' + idName + ' = ' + id
            + ';';
        $http.get("/cosyan/sql", {
          params : {
            sql : query,
            token : sessionStorage.getItem('token')
          }
        }).then(function success(response) {
          $scope.searchEntity();
        }, function error(response) {
          $scope.$error = response.data.error;
        });
      };
    });