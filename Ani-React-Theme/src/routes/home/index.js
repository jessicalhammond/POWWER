import React from 'react';

export default {

  path: '/',

  async action() {
    const Home = await new Promise((resolve) => {
      require.ensure([], (require) => {
        resolve(require('./Home').default);
      }, 'home');
    });
    return <Home key={'home'} />;
  },

};
