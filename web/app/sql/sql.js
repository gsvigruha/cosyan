'use strict';

angular.module('cosyan').controller('SQLCtrl', function($scope, $http) {

	$scope.run = function() {
		$http.get("/cosyan/sql", {
			params : {
				sql : $scope.query,
				token : sessionStorage.getItem('token'),
				session : sessionStorage.getItem('session'),
			}
		}).then(function success(response) {
			$scope.data = response.data;
			$scope.$error = undefined;
		}, function error(response) {
			$scope.data = undefined;
			$scope.$error = response.data.error;
		});
	};

	$scope.cancel = function() {
		$http.get("/cosyan/cancel", {
			params : {
				token : sessionStorage.getItem('token'),
				session : sessionStorage.getItem('session'),
			}
		});
	};

	$scope.createSession = function() {
		$http.get("/cosyan/createSession", {
			params : {
				token : sessionStorage.getItem('token'),
			}
		}).then(function success(response) {
			sessionStorage.setItem('session', response.data.session);
			$scope.$error = undefined;
		}, function error(response) {
			sessionStorage.removeItem('session');
			$scope.$error = response.data.error;
		});
	}
	$scope.createSession();
});