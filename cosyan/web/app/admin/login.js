'use strict';

angular.module('cosyan').controller('LoginCtrl', function($scope, $http) {
	$scope.login = function() {
		$http.get("/cosyan/login", {
			params : {
				username : $scope.username,
				password : $scope.password,
				method : 'LOCAL',
			}
		}).then(function success(response) {
			sessionStorage.setItem('user', response.data.token);
			$scope.$error = undefined;
			$scope.loggedIn = true;
		}, function error(response) {
			sessionStorage.removeItem('user');
			$scope.$error = response.data.error;
		});
	};

	$scope.logout = function() {
		$http.get("/cosyan/logout", {
			params : {
				user : sessionStorage.getItem('user')
			}
		}).then(function success(response) {
			sessionStorage.removeItem('user');
			$scope.$error = undefined;
			$scope.loggedIn = false;
		}, function error(response) {
			$scope.$error = response.data.error;
		});
	};
});