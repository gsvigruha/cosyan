'use strict';

angular.module('cosyan')
.controller('SettingsCtrl', function($scope, $http, $document, util) {
	$scope.loadSettings = function() {
	  util.settings(function(s) {
	    $scope.settings = s;
	  });
	};

	$scope.loadSettings();
});