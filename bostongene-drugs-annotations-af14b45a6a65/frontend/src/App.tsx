import React from 'react';
import {
  BrowserRouter as Router,
  Switch,
  Route,
  Link,
  useRouteMatch,
  useParams
} from 'react-router-dom';
import DrugsList from 'pages/DrugsList';
import Drug from 'pages/Drug';
import './App.scss';
import Experimental from 'pages/Experimental';


const App = () => {
  return (
    <div className="container">
      <h1>Table of possible candidates</h1>
      <Router>
        <Route
          path="/drugs/:drugId"
          render={({ match }) => <Experimental drugId={match.params.drugId} />}
        />
        <Route exact path="/" render={() => <DrugsList />} />
      </Router>
    </div>
  );
};

export default App;
