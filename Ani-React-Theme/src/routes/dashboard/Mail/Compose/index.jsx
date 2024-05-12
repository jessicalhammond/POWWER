import React from 'react';
// import Compose from './Compose';

export default {

  path: '/dashboard/compose',

  async action() {
    const Compose = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Compose').default), 'compose');
    });
    return <Compose key={'compose'} />;
  },
};
