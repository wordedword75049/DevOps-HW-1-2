import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import 'flexboxgrid2/flexboxgrid2.css';
import axios from 'axios';

const rootElement = document.getElementById('root');



(() => {
    const devBaseUrl = 
       '/local'  
    const defaultHeaders = {
        'X-Requested-With': 'XMLHttpRequest',
        Accept: 'application/json'
      };
  
    const developmentHeaders = {
      'X-Forwarded-Proto': 'http',
      'X-Forwarded-Host': 'localhost',
      'X-Forwarded-Port': '3000'
    };

      axios.defaults.headers.common = {
        ...developmentHeaders,
        ...defaultHeaders
      };
      axios.defaults.baseURL = devBaseUrl;
      ReactDOM.render(<App />, rootElement);
    })();
  