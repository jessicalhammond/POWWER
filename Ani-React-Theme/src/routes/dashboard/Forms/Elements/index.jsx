import React from 'react';
// import NProgress from 'nprogress';
// import Elements from './Elements';

export default {

  path: '/dashboard/elements',

  async action() {
    const Elements = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Elements').default), 'elements');
    });
    return <Elements key={'formElements'} />;
  },

};
