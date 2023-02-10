import React from 'react';
import { Button } from 'components/common/Button/Button';
import { useForm, FormProvider } from 'react-hook-form';
import { yupResolver } from '@hookform/resolvers/yup';
import formSchema from 'components/Wizard/schema';
import { StyledForm } from 'components/common/Form/Form.styled';

import * as S from './WizardForm.styled';
import KafkaCluster from './KafkaCluster/KafkaCluster';
import Authentication from './Authentication/Authentication';
import SchemaRegistry from './SchemaRegistry/SchemaRegistry';

type SecurityProtocol = 'SASL_SSL' | 'SASL_PLAINTEXT';

export type BootstrapServer = {
  host: string;
  port: string;
};
export type SchemaRegistryType = {
  url?: string;
  isAuth: boolean;
  username?: string;
  password?: string;
};
export type ClusterConfigFormValues = {
  name: string;
  readOnly: boolean;
  bootstrapServers: BootstrapServer[];
  useTruststore: boolean;
  truststore?: {
    location: string;
    password: string;
  };

  securityProtocol?: SecurityProtocol;
  authentication: {
    method: 'none' | 'sasl';
  };
  schemaRegistry?: SchemaRegistryType;
  properties?: Record<string, string>;
};

interface WizardFormProps {
  initialValues?: Partial<ClusterConfigFormValues>;
}

const CLUSTER_CONFIG_FORM_DEFAULT_VALUES: Partial<ClusterConfigFormValues> = {
  bootstrapServers: [{ host: '', port: '' }],
  useTruststore: false,
  authentication: {
    method: 'none',
  },
};

const Wizard: React.FC<WizardFormProps> = ({ initialValues }) => {
  const methods = useForm<ClusterConfigFormValues>({
    mode: 'all',
    resolver: yupResolver(formSchema),
    defaultValues: {
      ...CLUSTER_CONFIG_FORM_DEFAULT_VALUES,
      ...initialValues,
    },
  });

  const onSubmit = (data: ClusterConfigFormValues) => {
    // eslint-disable-next-line no-console
    console.log('SubmitData', data);
    return data;
  };

  const onReset = () => {
    methods.reset();
  };

  console.log('Errors:', methods.formState.errors);

  return (
    <FormProvider {...methods}>
      <StyledForm onSubmit={methods.handleSubmit(onSubmit)}>
        <KafkaCluster />
        <hr />
        <Authentication />
        <hr />
        <SchemaRegistry />
        <hr />
        <S.Section>
          <S.SectionName>Kafka Connect</S.SectionName>
          <div>
            <Button buttonSize="M" buttonType="primary">
              Add Kafka Connect
            </Button>
          </div>
        </S.Section>
        <S.Section>
          <S.SectionName>JMX Metrics</S.SectionName>
          <div>
            <Button buttonSize="M" buttonType="primary">
              Configure JMX Metrics
            </Button>
          </div>
        </S.Section>
        <div style={{ paddingTop: '10px' }}>
          <div
            style={{
              justifyContent: 'center',
              display: 'flex',
              gap: '10px',
            }}
          >
            <Button buttonSize="M" buttonType="primary" onClick={onReset}>
              Reset
            </Button>
            <Button type="submit" buttonSize="M" buttonType="primary">
              Save
            </Button>
          </div>
        </div>
      </StyledForm>
    </FormProvider>
  );
};

export default Wizard;
