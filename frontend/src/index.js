import React from 'react';
import ReactDOM from 'react-dom';
import {
	BrowserRouter as Router,
	Route,
	Switch
} from 'react-router-dom';

import HomePage from './pages/HomePage';
import SearchResult from './pages/SearchResults';
import 'antd/dist/antd.css';
import "bootstrap/dist/css/bootstrap.min.css";
import "shards-ui/dist/css/shards.min.css"
import ShoppingPage from './pages/ShoppingPage';
import ImagePage from './pages/ImagePage';


ReactDOM.render(
	<div>
		<Router>
			<Switch>
				<Route exact
					path="/"
					render={() => (
						<HomePage />
					)} />
				<Route exact
					path="/search"
					render={() => (
						<SearchResult />
					)} />
				<Route exact
					path="/shopping"
					render={() => (
						<ShoppingPage />
					)} />
				<Route exact
					path="/image-search"
					render={() => (
						<ImagePage />
					)} />
			</Switch>
		</Router>
	</div>,
	document.getElementById('root')
);

