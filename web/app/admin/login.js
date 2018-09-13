'use strict';

angular.module('cosyan').controller('LoginCtrl', function($scope, $http, util) {
  $scope.login = function() {
    $http.get("/cosyan/login", {
      params : {
        username : $scope.username,
        password : $scope.password,
        method : 'LOCAL',
      }
    }).then(function success(response) {
      sessionStorage.setItem('token', response.data.token);
      $scope.$error = undefined;
      $scope.loggedIn = true;
    }, function error(response) {
      sessionStorage.removeItem('token');
      $scope.$error = response.data.error;
    });
  };

  $scope.logout = function() {
    $http.get("/cosyan/logout", {
      params : {
        token : sessionStorage.getItem('token'),
      }
    }).then(function success(response) {
      sessionStorage.removeItem('token');
      $scope.$error = undefined;
      $scope.loggedIn = false;
    }, function error(response) {
      $scope.$error = response.data.error;
    });
  };

  util.settings(function(s) {
    $scope.auth = s.AUTH;
  });
});