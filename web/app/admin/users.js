'use strict';

angular.module('cosyan')
.controller('UsersCtrl', function($scope, $http, $document) {
  $scope.loadUsers = function() {
	$http.get("/cosyan/users", {
      params: { token: sessionStorage.getItem('token') }
    }).then(function(response) {
      $scope.users = response.data;
    });
  };
  
  $scope.loadUsers();
});