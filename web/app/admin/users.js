'use strict';

angular.module('cosyan')
.controller('UsersCtrl', function($scope, $http, $document, util) {
  $scope.newGrant = {};
  $scope.loadUsers = function() {
	$http.get("/cosyan/users", {
      params: { token: sessionStorage.getItem('token') }
    }).then(function(response) {
      $scope.users = response.data;
    });
  };

  $scope.runQuery = function(query) {
    $http.get("/cosyan/sql", {
	  params: { sql: query, token: sessionStorage.getItem('token') }
    }).then(function success(response) {
	  $scope.$error = undefined;
      $scope.loadUsers();
	}, function error(response) {
	  $scope.$error = response.data.error;
	});  
  };

  $scope.addGrant = function(username) {
    var query = 'grant ' + $scope.newGrant[username].method +
	  ' on ' + $scope.newGrant[username].tableOwner + '.' + $scope.newGrant[username].tableName +
	  ' to ' + username;
    if ($scope.newGrant[username].withGrantOption) {
      query = query + ' with grant option';
    }
	$scope.runQuery(query + ';');
  };

  $scope.loadUsers();
});