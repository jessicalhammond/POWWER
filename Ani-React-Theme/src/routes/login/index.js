

import React from 'react';
// import Login from './Login';

export default {

  path: '/login',

  async action() {
    const Login = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Login.jsx').default), 'login');
    });
    return <Login key={'login'} />;
  },

};
