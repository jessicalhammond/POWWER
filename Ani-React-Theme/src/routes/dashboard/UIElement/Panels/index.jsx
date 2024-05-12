import React from 'react';
// import NProgress from 'nprogress';
// import Panels from './Panels';

export default {

  path: '/dashboard/panels',

  async action() {
    const Panels = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Panels').default), 'panels');
    });
    return <Panels key={'panels'} />;
  },

};
