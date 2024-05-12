import React from 'react';
// import Blank from './Blank';

export default {

  path: '/dashboard/blank',

  async action() {
    const Blank = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Blank').default), 'blank');
    });
    return <Blank key={'blank'} />;
  },
};
