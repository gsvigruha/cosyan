'use strict';

angular.module('cosyan')
.controller('HelpCtrl', function($scope, $http) {
  $http.get("/help/list").then(function success(response) {
	$scope.pages = response.data;
  });
  
  $scope.loadHelp = function(name) {
    $scope.activePage = 'help/' + name + '.html';
  };
});